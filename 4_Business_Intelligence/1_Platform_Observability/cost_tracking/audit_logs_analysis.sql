-- Databricks notebook source
-- SKILLAIPPATH AUDIT LOGS ANALYSIS
-- Monitor access patterns and security events

SELECT 
  DATE(event_time) as event_date,
  service_name,
  action_name,
  user_identity.email as user_email,
  COUNT(*) as event_count,
  COUNT(DISTINCT user_identity.email) as unique_users
FROM system.access.audit
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
  AND workspace_id = 'YOUR_WORKSPACE_ID'
GROUP BY DATE(event_time), service_name, action_name, user_identity.email
ORDER BY event_date DESC, event_count DESC;
