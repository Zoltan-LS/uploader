./.venv/bin/rq worker uploads --name small-worker & 
./.venv/bin/rq worker large-files --name large-worker & 
./.venv/bin/rq worker cleanup --name cleanup-worker &

