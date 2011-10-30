require 'active_record'

module Delayed
  module Backend
    module ActiveRecord
      class FailedToParse < StandardError; end;
      
      # A job object that is persisted to the database.
      # Contains the work object as a YAML field.
      class Job < ::ActiveRecord::Base
        include Delayed::Backend::Base
        set_table_name :delayed_jobs

        before_save :set_default_run_at

        scope :ready_to_run, lambda {|worker_name, max_run_time|
          where(['(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR locked_by = ?) AND failed_at IS NULL', db_time_now, db_time_now - max_run_time, worker_name])
        }
        scope :by_priority, order('priority ASC, run_at ASC')
        def self.before_fork
          ::ActiveRecord::Base.clear_all_connections!
        end

        def self.after_fork
          ::ActiveRecord::Base.establish_connection
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
        end

        def parse_at(at)
          return unless at
          case at
          when /^(\d{1,2}):(\d\d)$/
            hour = $1.to_i
            min  = $2.to_i
            raise FailedToParse, at if hour >= 24 || min >= 60
            [hour, min]
          when /^\*{1,2}:(\d\d)$/
            min = $1.to_i
            raise FailedToParse, at if min >= 60
            [nil, min]
          else
            raise FailedToParse, at
          end
        end

        def time?(last, t, period, at_string)
          at = parse_at(at)
          ellapsed_ready = (last.nil? or (t - last).to_i >= period)
          time_ready = (at.nil? or ((at[0].nil? or t.hour == at[0]) and t.min == at[1]))
          ellapsed_ready and time_ready
        end

        # Find a few candidate jobs to run (in case some immediately get locked by others).
        def self.find_available(worker_name, limit = 5, max_run_time = Worker.max_run_time, last = nil)
          scope = self.ready_to_run(worker_name, max_run_time)
          scope = scope.scoped(:conditions => ['priority >= ?', Worker.min_priority]) if Worker.min_priority
          scope = scope.scoped(:conditions => ['priority <= ?', Worker.max_priority]) if Worker.max_priority

          ::ActiveRecord::Base.silence do
            scope.by_priority.all(:limit => limit).select do |job|
              result = true
              unless job.period.nil?
                time?(last, Time.now, job.period, job.at)
              end
              result
            end
          end
        end

        # Lock this job for this worker.
        # Returns true if we have the lock, false otherwise.
        def lock_exclusively!(max_run_time, worker)
          now = self.class.db_time_now
          affected_rows = if locked_by != worker
            # We don't own this job so we will update the locked_by name and the locked_at
            self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?) and (run_at <= ?)", id, (now - max_run_time.to_i), now])
          else
            # We already own this job, this may happen if the job queue crashes.
            # Simply resume and update the locked_at
            self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
          end
          if affected_rows == 1
            self.locked_at = now
            self.locked_by = worker
            self.locked_at_will_change!
            self.locked_by_will_change!
            return true
          else
            return false
          end
        end

        # Get the current time (GMT or local depending on DB)
        # Note: This does not ping the DB to get the time, so all your clients
        # must have syncronized clocks.
        def self.db_time_now
          if Time.zone
            Time.zone.now
          elsif ::ActiveRecord::Base.default_timezone == :utc
            Time.now.utc
          else
            Time.now
          end
        end

      end
    end
  end
end
