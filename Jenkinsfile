pipeline {
    agent any

    environment {
        AIRFLOW_URL = 'http://airflow-webserver:8080'
        AIRFLOW_USER = 'admin'
        AIRFLOW_PASSWORD = 'admin'
        MONGO_URI = 'mongodb://mongodb:27017/'
        DAG_ID = 'ecommerce_sales_pipeline'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install dependencies') {
            steps {
                sh 'pip3 install --no-cache-dir --break-system-packages -r requirements.txt'
            }
        }

        stage('Run tests') {
            steps {
                sh 'python3 -m pytest tests/ -v'
            }
        }

        stage('Validate DAG') {
            steps {
                sh 'python3 -m py_compile dags/*.py'
            }
        }

        stage('Deploy DAG') {
            steps {
                sh '''
                    cp dags/ecommerce_sales_pipeline.py /deploy/dags/
                    cp scripts/pipeline_logic.py /deploy/scripts/
                    cp scripts/__init__.py /deploy/scripts/ || true
                '''
            }
        }

        stage('Trigger DAG') {
            steps {
                sh '''
                    curl -u ${AIRFLOW_USER}:${AIRFLOW_PASSWORD} -X PATCH "${AIRFLOW_URL}/api/v1/dags/${DAG_ID}" \
                         -H "Content-Type: application/json" -d "{\\"is_paused\\": false}"

                    curl -u ${AIRFLOW_USER}:${AIRFLOW_PASSWORD} -X POST "${AIRFLOW_URL}/api/v1/dags/${DAG_ID}/dagRuns" \
                         -H "Content-Type: application/json" -d "{}"

                    echo "Attente de la fin d'execution du DAG..."
                    sleep 60
                '''
            }
        }

        stage('Verify MongoDB') {
            steps {
                sh 'python3 scripts/check_mongodb.py --dag-id ${DAG_ID} --max-age-minutes 10'
            }
        }
    }

    post {
        success {
            echo 'Pipeline execute avec succes.'
        }
        failure {
            echo 'Echec du pipeline - voir les logs ci-dessus.'
        }
    }
}