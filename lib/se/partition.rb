module SE
  module Partition

    #
    # Create a partition for a model by field.
    #
    # call-seq:
    #   SE::Partition::partition(model, field, options) => true|false
    #
    #   +model+ - Model to partition.
    #   +field+ - Field of model to partition on.
    #
    #   Options:
    #     +perms+       - Hash of additional permissions to grant to new tables.
    #     +interval+    - For Date fields: how frequently to create a new partition table.
    #                     Possible values are <tt>day</tt>, <tt>week</tt>, <tt>month</tt> and <tt>year</th>.
    #
    # ==== Examples
    #
    #   # Partition User table by YYYYMMDD
    #   SE::Partition::partition(User, :created_at)
    #
    #   # Partition User table by YYYYMM
    #   SE::Partition::partition(User, :created_at, :interval => :month)
    #
    #   # Partition User by type and grant metrics user select and update permissions
    #   SE::Partition::Date.partition(User, :type, :perms => {'metrics' => 'select, update'})
    #
    def self.partition(model, field, opts={})
      "SE::Partition::#{adapter.classify}".constantize.partition(model, field, opts)
    end

    #
    # Prune old partitions.
    #
    # Note: This method only works when a model is partitioned by a date field.
    #
    # call-seq:
    #   SE::Partition::prune(model, keep_count) => dropped table count
    #
    #   +model+      - Model to prune.
    #   +keep_count+ - Number of tables to keep.
    #
    def self.prune(model, keep_count)
      "SE::Partition::#{adapter.classify}".constantize.prune(model, keep_count)
    end

    def self.adapter
      @adapter ||= ActiveRecord::Base.configurations.find {|env, config| config.has_key?('adapter')}[1]['adapter']
    end

  end # module Partition
end # module SE
