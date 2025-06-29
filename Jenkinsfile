pipeline {
    agent any

    environment {
        GCP_PROJECT_ID = 'pro-env-test'
        GCS_BUCKET_NAME = 'asia-south1-cloud-publishin-9441e4f8-bucket'
        COMPOSER_DAGS_FOLDER = 'dags'
    }

    tools {
        // Make sure you have a Python installation configured in Jenkins Global Tool Configuration
        python 'python3'
    }

    stages {
        stage('Checkout') {
            steps {
                // Clones the repository 
                git 'https://github.com/PatilNithin/prod-etl-pipeline.git'
            }
        }

        stage('Linting') {
            steps {
                sh 'pip install pylint'
                sh 'pylint dags/*.py scripts/*.py'
            }
        }

        stage('Unit Tests') {
            steps {
                // Install dependencies for testing
                sh 'pip install pytest'
                sh 'pip install faker'
                // You might need to install pyspark locally for some tests
                sh 'pip install pyspark'

                // Run tests
                sh 'pytest tests/'
            }
        }

        stage('Deploy to Staging GCS') {
            steps {
                withCredentials([file(credentialsId: 'gcp-credentials', variable: 'GCP_KEY')]) {
                    sh """
                    gcloud auth activate-service-account --key-file=\$GCP_KEY
                    gsutil -m cp -r dags/* gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/
                    gsutil -m cp -r scripts/* gs://${GCS_BUCKET_NAME}/staging/scripts/
                    """
                }
            }
        }

        stage('Deploy to Production Composer') {
            steps {
                script {
                    // This input step provides a manual gate before deploying to production.
                    // You can remove this for fully automated deployments.
                    input "Deploy to production?"
                }
                withCredentials([file(credentialsId: 'gcp-credentials', variable: 'GCP_KEY')]) {
                    sh """
                    gcloud auth activate-service-account --key-file=\$GCP_KEY

                    // This is the key to not disturbing running jobs. We copy to a final location from staging.
                    // Airflow's sync mechanism will pick up the changes atomically.
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/* gs://${GCS_BUCKET_NAME}/${COMPOSER_DAGS_FOLDER}/
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/scripts/* gs://${GCS_BUCKET_NAME}/${COMPOSER_DAGS_FOLDER}/
                    """
                }
            }
        }
    }
}