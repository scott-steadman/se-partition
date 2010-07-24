module SE
  module Partition
    class Postgresql

      attr_reader :model, :field, :opts

      def initialize(model, field=nil, opts={})
        @model = model
        @field = field
        @opts = opts

        model.connection.client_min_messages = (verbose? ? 'info' : 'panic')
      end

      def self.partition(model, field, opts)
        new(model, field, opts).partition
      end

      def partition

        ensure_language

        case field_type
          when :datetime  : partition_by_date
          when :string    : partition_by_string
          else raise "Unable to partition based on #{model}.#{field} of type #{field_type}"
        end
      end

      def self.prune(model, keep_count)
        new(model).prune(keep_count)
      end

      def prune(keep_count)
        partitions = model.connection.tables.select {|ii| ii =~ /^#{table_name}_\d{4,8}$/}.sort
        partitions[0 .. -(keep_count + 1)].each do |ii|
          puts "Dropping: #{ii}" unless Rails.env.test?
          exec("DROP TABLE #{model.connection.quote_table_name(ii)} CASCADE")
        end
        [0, partitions.size - keep_count].max
      end

    private

      attr_accessor :sql_partition_type

      def field_type
        model.columns_hash[field.to_s].type
      end

      def partition_by_date
        self.sql_partition_type = 'date'

        template = case interval
          when 'year'  then '_YYYY'
          when 'month' then '_YYYYMM'
          else '_YYYYMMDD'
        end

        exec(%Q{
          CREATE OR REPLACE FUNCTION partition_for_#{table_name} (partition_date date) RETURNS "name" AS
          $BODY$
            DECLARE
              partition_beginning_date CONSTANT date := date_trunc('#{interval}', partition_date)::#{sql_partition_type};
              needed_partition_table_name CONSTANT "name" := '#{table_name}' || to_char(partition_beginning_date, '#{template}');
            BEGIN

              #{check_for_table}

              if not found then
                DECLARE
                  base_table_name CONSTANT text := quote_ident('#{table_name}');
                  quoted_column_name CONSTANT text := quote_ident('#{field}');
                  partition_beginning_date CONSTANT date := date_trunc('#{interval}', partition_date)::#{sql_partition_type};
                  next_partition_beginning_date date := date_trunc('#{interval}', partition_date + ('1 #{interval}')::interval)::#{sql_partition_type};
                  quoted_needed_table_name CONSTANT text := quote_ident (needed_partition_table_name);
                  quoted_rule_name CONSTANT text := quote_ident('rule_' || needed_partition_table_name);
                  base_table_owner name;
                  s text;
                  a text;
                  parent_index_name text;
                  parent_index_has_valid_name boolean;
                BEGIN

                  #{extract_table_owner}

                  #{check_for_partitioning_column}

                  s := $$
                    CREATE TABLE $$ || quoted_needed_table_name || $$ (
                      CHECK ( $$ || quoted_column_name || $$ >= DATE $$ || quote_literal( partition_beginning_date ) || $$ AND
                              $$ || quoted_column_name || $$ < DATE $$ || quote_literal( next_partition_beginning_date ) || $$ )
                    ) INHERITS ( $$ || base_table_name || $$ ); $$;
                  raise notice 'creating table as [%]', s;
                  EXECUTE s;

                  #{change_owner_of_new_table}

                  #{add_permissions}

                  #{copy_indexes}

                  -- now we create a rule, that will be assigned to the original table
                  s := $$
                    CREATE RULE $$ || quoted_rule_name || $$ AS
                    ON INSERT TO $$ || base_table_name || $$
                      WHERE ( $$ || quoted_column_name || $$ >= DATE $$ || quote_literal( partition_beginning_date ) || $$ AND
                              $$ || quoted_column_name || $$ < DATE $$ || quote_literal( next_partition_beginning_date ) || $$ )
                    DO INSTEAD
                      INSERT INTO $$ || quoted_needed_table_name || $$ VALUES (NEW.*); $$;
                  -- raise notice 'creating a rule as [%]', s;
                  EXECUTE s;
                END;
              end if;
              return needed_partition_table_name;
            END;
          $BODY$
          LANGUAGE plpgsql strict volatile;
        })

        create_partition_insert_function

        create_insertion_trigger

      end

      def partition_by_string
        self.sql_partition_type = 'text'

        exec(%Q{
          CREATE OR REPLACE FUNCTION partition_for_#{table_name} (partition_string #{sql_partition_type}) RETURNS "name" AS
          $BODY$
            DECLARE
              needed_partition_table_name CONSTANT "name" := '#{table_name}_' || lower(partition_string);
            BEGIN

              #{check_for_table}

              IF NOT FOUND THEN
                DECLARE
                  base_table_name CONSTANT text := quote_ident('#{table_name}');
                  quoted_column_name CONSTANT text := quote_ident('#{field}');
                  quoted_needed_table_name CONSTANT text := quote_ident (needed_partition_table_name);
                  quoted_rule_name CONSTANT text := quote_ident('rule_' || needed_partition_table_name);
                  base_table_owner name;
                  s text;
                  a text;
                  parent_index_name text;
                  parent_index_has_valid_name boolean;
                BEGIN

                  #{extract_table_owner}

                  #{check_for_partitioning_column}

                  -- create partition with constraint check
                  s := $$
                    CREATE TABLE $$ || quoted_needed_table_name || $$ (
                      CHECK ( lower($$ || quoted_column_name || $$) = TEXT $$ || quote_literal( lower(partition_string) ) || $$ )
                    ) INHERITS ( $$ || base_table_name || $$ ); $$;
                  raise notice 'creating table as [%]', s;
                  EXECUTE s;

                  #{change_owner_of_new_table}

                  #{add_permissions}

                  #{copy_indexes}

                  -- now we create a rule, that will be assigned to the original table
                  s := $$
                    CREATE RULE $$ || quoted_rule_name || $$ AS
                    ON INSERT TO $$ || base_table_name || $$
                      WHERE ( lower($$ || quoted_column_name || $$) = TEXT $$ || quote_literal( partition_string ) || $$ )
                    DO INSTEAD
                      INSERT INTO $$ || quoted_needed_table_name || $$ VALUES (NEW.*); $$;
                  -- raise notice 'creating a rule as [%]', s;
                  EXECUTE s;
                END;
              end if;
              return needed_partition_table_name;
            END;
          $BODY$
          LANGUAGE plpgsql strict volatile;
        })

        create_partition_insert_function

        create_insertion_trigger

      end

      def ensure_language(language='plpgsql')
        if 0 == result = exec("SELECT true FROM pg_language WHERE lanname=#{model.connection.quote(language)}").count
          exec("CREATE LANGUAGE #{language}")
        end
      end

      def check_for_table
        %Q{
            -- check that the needed table exists on the database
            perform 1
              from pg_class, pg_namespace
              where relnamespace = pg_namespace.oid
                and relkind = 'r'::"char"
                and relname = needed_partition_table_name;
        }
      end

      def extract_table_owner
        %Q{
          -- check for the base table and extract the table owner
          select pg_roles.rolname into base_table_owner
            from pg_class, pg_namespace, pg_roles
           where relnamespace = pg_namespace.oid
             and relkind = 'r'::"char"
             and relowner = pg_roles.oid
             and relname = base_table_name;
          if not found then
            raise exception 'cannot find base table %', base_table_name;
          end if;
        }
      end

      def check_for_partitioning_column
        %Q{
          -- now check that the base table contains the partitioning column
          perform 1 from information_schema.columns where table_name = base_table_name and column_name = '#{field}';
          if not found then
            raise exception 'cannot find partitioning column % in the table %', quoted_column_name, base_table_name;
          end if;
        }
      end

      def change_owner_of_new_table
        %Q{
          if coalesce(length(base_table_owner), 0) = 0 then
            raise exception 'base_table_owner is unknown';
          end if;
          s := $$ ALTER TABLE $$ || quoted_needed_table_name || $$ OWNER TO $$ || base_table_owner;
          raise notice 'changing owner as [%]', s;
          EXECUTE s;
        }
      end

      def add_permissions
        sql = []

        perms.each do |user, permissions|
          sql << %Q{
            s := $$ GRANT #{permissions} ON $$ || quoted_needed_table_name || $$ TO #{user}; $$;
            raise notice 'granting #{user} permissions as [%]', s;
            EXECUTE s;
          }
        end

        sql.join("\n")
      end

      def copy_indexes
        %Q{
          -- extract all the indexes existing on the parent table and apply them to the newly created partition
          FOR a, s, parent_index_name, parent_index_has_valid_name
           IN SELECT CASE indisclustered WHEN TRUE THEN 'ALTER TABLE ' || needed_partition_table_name::text || ' CLUSTER ON ' || replace( i.relname, c.relname, needed_partition_table_name::text ) ELSE NULL END as clusterdef,
                      replace( pg_get_indexdef(i.oid), base_table_name::text, needed_partition_table_name::text ),
                      i.relname,
                      strpos( i.relname, base_table_name::text ) > 0
                 FROM pg_index x
                 JOIN pg_class c ON c.oid = x.indrelid
                 JOIN pg_class i ON i.oid = x.indexrelid
                 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
                WHERE c.relkind = 'r'::"char"
                  AND i.relkind = 'i'::"char"
                  AND c.relname = base_table_name
          LOOP
            IF parent_index_has_valid_name THEN
              RAISE NOTICE 'creating index as [%]', s;
              EXECUTE s;
              if a is not null then
                raise notice 'setting clustering as [%]', a;
                EXECUTE a;
              end if;
            else
              raise exception 'parent index name [%] should contain the name of the parent table [%]', parent_index_name, base_table_name;
            end if;
          end loop;
        }
      end

      def create_partition_insert_function
        exec(%Q{
          CREATE OR REPLACE FUNCTION #{table_name}_partition_insert_function()
            RETURNS TRIGGER AS
            $BODY$
              DECLARE
                partition_table text;
                s text;
              BEGIN
                if new.#{field} is null then
                  raise exception 'partitioning column "#{field}" cannot be NULL';
                end if;
                partition_table := partition_for_#{table_name}(new.#{field}::#{sql_partition_type});
                select new into s;
                s := $$ INSERT INTO $$ || partition_table ||
                     $$ SELECT ($$ || quote_literal( s ) || $$::$$ || '#{table_name}' || $$).*  $$;
                EXECUTE s;
                RETURN NULL;
              END;
            $BODY$
          LANGUAGE plpgsql;
        })
      end

      def create_insertion_trigger
        exec(%Q{
          CREATE TRIGGER #{table_name}_partition_insert_trigger
            BEFORE INSERT ON #{table_name}
            FOR EACH ROW EXECUTE PROCEDURE #{table_name}_partition_insert_function();
        })
      end

      def exec(sql)
        puts "Executing:\n#{sql}" if verbose?
        model.connection.execute(sql)
      end

      def quote(string)
        model.connection.quote(string.to_s)
      end

      def table_name
        table_name = opts[:table_name] || model.table_name
      end

      def interval
        (opts[:interval] || 'day').to_s
      end

      def verbose?
        opts[:verbose]
      end

      def perms
        opts[:perms] || opts[:permissions] || {}
      end

    end # class Postgresql
  end # module Partition
end # module SE
