-- =============================================================
-- Migration: Backfill missing SCHEDULED events with descriptors
-- =============================================================
-- Migration 20260228120000 backfilled job_events with one event
-- per existing job using the job's current status as event_type.
-- Jobs that were already past SCHEDULED (e.g. RUNNING, COMPLETED)
-- only received a non-SCHEDULED event. The descriptor backfill in
-- migration 20260306120000 only targeted SCHEDULED events, so
-- those jobs were missed.
--
-- This migration inserts a SCHEDULED event with the descriptor
-- for any job that has a non-null descriptor in the jobs table
-- but no SCHEDULED event with a non-null detail in job_events.
-- =============================================================

INSERT INTO job_events (created_at, job_id, node_id, event_type, detail)
SELECT j.created_at, j.id, js.node_id, 'SCHEDULED', j.descriptor
FROM jobs j
INNER JOIN jobs_status js ON j.id = js.job_id
WHERE j.descriptor IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM job_events je
    WHERE je.job_id = j.id
      AND je.event_type = 'SCHEDULED'
      AND je.detail IS NOT NULL
  );
