.PHONY:	setup test download-olist clean

setup:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

test:
	pytest -v

download-olist:
	mkdir -p data/raw/olist
	kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/olist
	unzip -o data/raw/olist/brazilian-ecommerce.zip -d data/raw/olist

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
