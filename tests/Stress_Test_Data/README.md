# Stress_Test_Data

The stress suite (`../stress/test_many_jobs.py`) generates its inputs from a fixed random seed (2026), so no data
files are checked in and every run submits the same jobs. Set `APS_STRESS_JOBS` to change the burst size
(default 150). Put any larger fixed corpora (long videos, big text files) for future stress tests here.
