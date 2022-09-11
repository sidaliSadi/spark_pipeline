# Flight Radar
Spark pipeline on Flight Radar API

## Usage

1. Create virtual envirenemnt  `python3 -m venv v_env`
2. Activate v_env `source v_env/bin/activate `
3. Run `git clone https://github.com/sidaliSadi/spark_pipeline.git`
4. Run `cd spark_pipeline`
5. Run the `pip install -r requirements.txt` to install dependencies


### Code folder architecture

```
.
├── data
│   └── clean                       data folder containing extracted (clean) files airports and airlines
    └── without_cleaning            data folder containing extracted (not clean) files flights_details
└── src
    ├── common
        ├──  init_spark.py                pyspark initialization

    ├── extract.py                  extract data from FlightRadar API and save it
    └── transform.py                transform flights_details data
└── answer_questions.ipynb          jupyter notebook for launch transformations and answering the questions
```