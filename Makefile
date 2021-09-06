.PHONY: report test_cli

report:
	pytest --cov=. --html=tests/reports/report.html --cov-report html -W error tests/

ci:
	pytest --cov=. -W error tests/

test_cli:
	python pypel/main.py -c conf_template.json -f ./tests/fake_data/test_init_df.csv
	python pypel/main.py -c conf_template.json -f ./tests/fake_data/test_init_df.csv -p EXAMPLE
	python pypel/main.py -c conf_template.json -f ./tests/fake_data/test_init_df.csv -p EXAMPLE -i pypel_change_indice
