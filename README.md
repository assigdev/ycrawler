# Ycrawler

### Install


    git clone https://github.com/assigdev/ycrawler.git
    
    pipenv install
    
if you don't have pipenv:
    
    pip install pipenv

### Run

    python crawler.py

### Configs
    
    optional arguments:
      -h, --help            show this help message and exit
      -st STATE, --state STATE
                            state file path
      -sl SLEEP_TIME, --sleep_time SLEEP_TIME
                            time for before next parse
      -o OUTPUT, --output OUTPUT
                            output path
      -t TIMEOUT, --timeout TIMEOUT
                            time out for async tasks
      -l LOG, --log LOG     log file path
      -d, --debug           debug logging
