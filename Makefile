.PHONY: report local_elastic

report:
	pytest --cov=. --html=tests/reports/report.html --cov-report html -W error tests/

ci:
	pytest --cov=. -W error tests/

local_elastic:
	python pypel/main.py -f conf_template.json
