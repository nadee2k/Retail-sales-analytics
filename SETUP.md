# Project setup

Follow these steps to prepare the environment and run notebooks locally.

1. Create virtual environment (Linux/Mac):

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Place the Superstore CSV as `data/raw_data.csv`.

3. Run the cleaning notebook (from project root):

```bash
jupyter notebook
# open notebooks/01_data_cleaning.ipynb and run cells
```

4. After cleaning, open `notebooks/02_eda_analysis.ipynb` and `notebooks/03_statistical_tests.ipynb`.
