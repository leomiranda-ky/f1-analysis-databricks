-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1lake/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;
