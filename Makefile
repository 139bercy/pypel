.PHONY: report

report:
	pytest --cov=. --html=tests/reports/report.html --cov-report html -W error tests/
