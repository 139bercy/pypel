.PHONY: report

report:
	pytest --cov=. --html=tests/reports/report.html --cov-report html:tests/reports/ --cov-branch tests/
