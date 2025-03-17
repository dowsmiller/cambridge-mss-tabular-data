## Setting up and running script
### Mac Users

**Step 1:** Clone repository

**Step 2:** Check if python and pip is installed

```
python3 --version
pip3 --version
```

**Step 3:** Create a virtual environment 

```
python3 -m venv .venv
```

**Step 4:** Activate source

```
source .venv/bin/activate
``` 

**Step 5:** Install dependencies

```
pip install -r requirements.txt
```

**Step 6:** Place input xml files inside `data` folder in root of project

**Step 7:** Changing configs (Optional)

Inside ```tei_processor.py``` you can change paths and database name

```
input_folder = "data/collections"
output_folder = "output"
db_name = "tei_data.db"
```

**Step 8:** Run processor script

```
python tei_processor.py
```

After processing is completed you should be able to see outputs in ```output``` folder


