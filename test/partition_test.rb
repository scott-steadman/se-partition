ENV["RAILS_ENV"] = "test"
require File.expand_path(File.dirname(__FILE__) + "/../config/environment")
require 'test_help'

class ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
  # partition triggers can't return values. :(
  def supports_insert_with_returning?
    false
  end
end

# lifed from db:test:prepare task
config = ActiveRecord::Base.configurations[RAILS_ENV]
ActiveRecord::Base.establish_connection(config.merge('database' => 'postgres', 'schema_search_path' => 'public'))
ActiveRecord::Base.connection.drop_database(config['database'])
ActiveRecord::Base.connection.create_database(config['database'], config)

class CreateModels < ActiveRecord::Migration
  def self.up
    create_table :date_models do |t|
      t.datetime :key
    end
    add_index :date_models, :key, :unique => true

    create_table :string_models do |t|
      t.string :key
    end
    add_index :string_models, :key, :unique => true
  end

  def self.down
    drop_table :date_models
    drop_table :string_models
  end
end

class DateModel < ActiveRecord::Base ; end
class StringModel < ActiveRecord::Base ; end

class PartitionTest < ActiveSupport::TestCase

  test 'date partition' do
    SE::Partition.partition(DateModel, :key, :verbose => false)
    assert_difference "DateModel.count", 3 do
      assert_difference "DateModel.connection.tables.count", 2 do
        DateModel.create!(:key => Time.now)
        DateModel.create!(:key => Time.now + 5.seconds)
        DateModel.create!(:key => Time.now + 1.day)
      end
    end
    DateModel.connection.tables.select {|ii| /date_models_/ =~ ii}.each do |table|
      assert_equal 2, DateModel.connection.indexes(table).size, 'there should be 2 indexes on the partition'
    end
  end

  test 'date prune' do
    SE::Partition.partition(DateModel, :key, :verbose => false)
    10.times do |ii|
      DateModel.create!(:key => Time.now - ii.days)
    end

    assert_difference "#{DateModel}.connection.tables.count", 0 do
      assert_equal 0, SE::Partition.prune(DateModel, 15), 'prune should return no tables dropped'
    end

    assert_difference "#{DateModel}.connection.tables.count", -5 do
      assert_equal 5, SE::Partition.prune(DateModel, 5), 'prune should return 5 tables dropped'
    end
  end

  test 'string partition' do
    SE::Partition.partition(StringModel, :key, :verbose => false)
    assert_difference "StringModel.count", 3 do
      assert_difference "StringModel.connection.tables.count", 2 do
        StringModel.create!(:key => 'one')
        StringModel.create!(:key => 'Two')
        StringModel.create!(:key => 'tWo')
      end
    end
    StringModel.connection.tables.select {|ii| /string_models_/ =~ ii}.each do |table|
      assert_equal 2, StringModel.connection.indexes(table).size, 'there should be 2 indexes on the partition'
    end
  end

end
