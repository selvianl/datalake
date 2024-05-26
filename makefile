CFG_ISORT := .isort.cfg
CFG_FLAKE := .flake.cfg
CFG_BLACK := .black.cfg

isort_check:
	isort --settings-path=$(CFG_ISORT) --check --diff .

black_check:
	black --config=$(CFG_BLACK) --check --diff .

isort:
	isort --settings-path=$(CFG_ISORT) .

black:
	black --config=$(CFG_BLACK) .

lint:
	flake8 --config=$(CFG_FLAKE) .

unused_imports:
	autoflake --remove-all-unused-imports -i -r .

format: unused_imports isort black