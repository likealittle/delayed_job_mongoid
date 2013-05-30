# encoding: utf-8
require 'delayed_job'
require 'mongoid'

module Delayed
  module Backend
    module Mongoid
      class Job
        include ::Mongoid::Document
        include ::Mongoid::Timestamps
        include Delayed::Backend::Base

        field :priority,    :type => Integer, :default => 0
        field :attempts,    :type => Integer, :default => 0
        field :handler,     :type => String
        field :run_at,      :type => Time
        field :locked_at,   :type => Time
        field :locked_by,   :type => String
        field :failed_at,   :type => Time
        field :last_error,  :type => String
        field :queue,       :type => String

        # nil or true.
        field :is_ready,    :type => Boolean

=begin
        State Machine:
        1. Waiting:
            is_ready = nil
            run_at < Time.now
            locked_at = locked_by = nil
            failed_at = nil
        2. Pending to execute:
            is_ready = true
            run_at = nil
            locked_at = locked_by = nil
            failed_at = nil
        3. Running:
            run_at = nil
            is_ready = nil
            locked_at != nil and locked_by != nil
            failed_at = nil
        4. Failed:
            run_at = is_ready = nil
            locked_at = locked_by = nil
            failed_at != nil
=end


        # When the worker comes up, find everyone who is locked by me, and unlock them.
        # Also used to unlocked workers who are locked for a long time.
        index :locked_by, sparse: true

        # Moving from `Pending to execute` state to `Running`
        index({:is_ready => 1, :priority => 1}, sparse: true)

        # Used to mark jobs ready to run as `is_ready` true.
        # Only `is_ready` of nil can have non-nil `run_at`
        # Moving from `Waiting` state to `Pending to execute`
        index :run_at, sparse: true

        before_save :set_default_run_at
        before_save :set_default_is_ready
        before_save :mark_failed

        def self.db_time_now
          Time.now.utc
        end

        def set_default_is_ready
          if run_at and run_at <= db_time_now
            self.is_ready = true
            self.run_at = nil
          end
          return true
        end

        def mark_failed
          return unless failed_at_changed? and failed_at_was.nil? and !failed_at.nil?
          self.run_at = self.is_ready = self.locked_at = self.locked_by = nil
        end

        # Reserves this job for the worker.
        def self.reserve(worker, max_run_time = Worker.max_run_time)
          housekeeping(worker, max_run_time)

          criteria = self.where(is_ready: true)

          criteria = criteria.gte(:priority => Worker.min_priority.to_i) if Worker.min_priority
          criteria = criteria.lte(:priority => Worker.max_priority.to_i) if Worker.max_priority
          criteria = criteria.any_in(:queue => Worker.queues) if Worker.queues.any?

          criteria.asc(:priority).find_and_modify(
            {"$set" => {:locked_at => db_time_now, :locked_by => worker.name,
              :run_at => nil, :is_ready => nil} }, :new => true
          )
        end

        def self.pending_to_execute_state
          {:locked_at => nil, :locked_by => nil, :is_ready => true}
        end

        @@last_housekeeping = nil
        def self.housekeeping(worker, max_run_time)
          unless @@last_housekeeping
            # The first time the worker boots up.
            clear_locks!(worker.name)
          end
          if rand(10) == 0
            # Once in 10 jobs, move from `Waiting` to `Pending to execute`
            self.where(is_ready: nil, run_at: {'$gt' => db_time_now}).
              update_all(pending_to_execute_state)
          end
          # Make sure we run housekeeping at-least once in a while, but not
          # too frequently.
          return if @@last_housekeeping and @@last_housekeeping < max_run_time.seconds.ago
          @@last_housekeeping = Time.now

          # If someone else holds bad locks, clear them too.
          self.where(:locked_by => {'$ne' => nil}, 
            :locked_at => {'$lt' => db_time_now - max_run_time}).
            update_all(pending_to_execute_state)
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          self.where(:locked_by => worker_name).update_all(pending_to_execute_state)
        end

        def reload(*args)
          reset
          super
        end
      end
    end
  end
end
