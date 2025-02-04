FROM python:3.12.7

ENV AIRFLOW_HOME=/usr/local/airflow

RUN apt-get update && \
	apt-get install -y gcc libc-dev vim procps supervisor && \
	rm -rf /var/lib/apt/lists/*

RUN pip install apache-airflow

RUN mkdir -p $AIRFLOW_HOME

WORKDIR $AIRFLOW_HOME

RUN airflow db init

COPY supervisord.conf /etc/supervisor/supervisord.conf

COPY my_dag.py $AIRFLOW_HOME/dags/

EXPOSE 8080

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]