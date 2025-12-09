python -m venv .venv

.\.venv\Scripts\activate

pip install -r requirements.txt

set POLL_CYCLE_SEC=120 (120 seconds)

set BATCH_SIZE=20 (500/20 means 25 calls 5 seconds apart)

set DATA_DELAY_NOTE=20 delay 20 seconds after all calls

uvicorn app:app --host 0.0.0.0 --port 8080 --reload
