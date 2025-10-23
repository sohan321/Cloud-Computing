python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

set POLL_CYCLE_SEC=20 (2 seconds)
set BATCH_SIZE=50 (500/50 means 10 calls 2 seconds apart)
set DATA_DELAY_NOTE=20 delay 20 seconds after all calls
uvicorn app:app --host 0.0.0.0 --port 8080 --reload
