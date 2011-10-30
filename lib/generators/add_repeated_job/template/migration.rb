class AddRepeatedJobAttributesToDelayedJobs < ActiveRecord::Migration
  def self.up
    add_column :delayed_jobs, :period, :integer
    add_column :delayed_jobs, :at, :string
    add_column :delayed_jobs, :last_run_at, :datetime
  end
end