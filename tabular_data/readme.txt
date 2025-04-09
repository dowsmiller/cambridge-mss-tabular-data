Mac Users

Step 1: Clone repository

Step 2: Open a Terminal window at 'tabular_data'

Step 3: Check if python and pip are installed:

	python3 --version
	pip3 --version

Step 4: Create a virtual environment:

	python3 -m venv .venv

Step 5: Activate source:

	source .venv/bin/activate

Step 6: Install dependencies:

	pip install -r requirements.txt

Step 7: Run processor script:

	python processor.py

After processing is completed, outputs should appear in the 'output' folder