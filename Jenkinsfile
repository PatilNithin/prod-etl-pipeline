pipeline {
    agent any // Or specify a label if you have dedicated Jenkins agents, e.g., agent { label 'my-python-agent' }

    environment {
        GCP_PROJECT_ID = 'pro-env-test'
        GCS_BUCKET_NAME = 'asia-south1-cloud-publishin-9441e4f8-bucket'
        COMPOSER_DAGS_FOLDER = 'dags' // This is typically 'dags' in Composer's GCS bucket
        // Define a variable for the Python executable for consistency
        PYTHON_EXEC = 'python3' // Use 'python' if 'python3' is not available or desired
        VENV_DIR = '.venv'
    }

    // REMOVED: The 'tools { python 'python3' }' block as it's not a recognized tool type for direct management.
    // Instead, we will rely on 'python3' being in the agent's PATH or specify its full path in 'sh' commands.

    stages {

        stage('Setup Python Environment') {
            steps {
                script {
                    echo "Creating Python virtual environment..."
                    // Create the virtual environment
                    sh "${PYTHON_EXEC} -m venv ${VENV_DIR}"

                    echo "Upgrading pip within the virtual environment..."
                    // Explicitly use the pip from the virtual environment
                    sh "${VENV_DIR}/bin/pip install --upgrade pip"

                    echo "Installing project dependencies from requirements.txt..."
                    // Install project dependencies into the virtual environment
                    sh "${VENV_DIR}/bin/pip install -r requirements.txt"
                }
            }
        }


        stage('Linting') {
            steps {
                // Run pylint on your Python files
                //sh "${PYTHON_EXEC} -m pylint dags/*.py scripts/*.py"
                sh "${VENV_DIR}/bin/pylint dags/*.py scripts/*.py"
            }
        }

        stage('Unit Tests') {
            steps {
                // Run pytest on your test files
                // pytest will automatically discover and run tests in the 'tests/' directory
                //sh "${PYTHON_EXEC} -m pytest tests/"
                sh "${VENV_DIR}/bin/pytest tests/"
            }
        }

        stage('Deploy to Staging GCS') {
            steps {
                // Use withCredentials to securely access your GCP service account key
                withCredentials([file(credentialsId: 'jenkins-service-acc', variable: 'GCP_KEY')]) {
                    sh """
                    # Authenticate gcloud with the service account key
                    gcloud auth activate-service-account --key-file=\$GCP_KEY --project=${GCP_PROJECT_ID}

                    # Copy DAGs to a 'staging' subfolder in your Composer DAGs bucket
                    gsutil -m cp -r dags/* gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/

                    # Copy scripts to a 'staging' subfolder for scripts (or within dags folder if they are part of dags)
                    // Note: If your Composer environment expects scripts in a different path (e.g., 'data' or 'plugins'), adjust this path.
                    gsutil -m cp -r scripts/* gs://${GCS_BUCKET_NAME}/staging/scripts/
                    """
                }
            }
        }

        stage('Deploy to Production Composer') {
            steps {
                script {
                    // This input step provides a manual gate before deploying to production.
                    // You can remove this for fully automated deployments if desired.
                    input message: "Approve deployment to production?", ok: "Deploy"
                }
                withCredentials([file(credentialsId: 'jenkins-service-acc', variable: 'GCP_KEY')]) {
                    sh """
                    # Authenticate gcloud with the service account key
                    gcloud auth activate-service-account --key-file=\$GCP_KEY --project=${GCP_PROJECT_ID}

                    // This is the key to not disturbing running jobs. We copy to a final location from staging.
                    // Airflow's sync mechanism will pick up the changes atomically.

                    # Copy DAGs from staging to the main Composer DAGs folder
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/${COMPOSER_DAGS_FOLDER}/* gs://${GCS_BUCKET_NAME}/${COMPOSER_DAGS_FOLDER}/

                    # Copy scripts from staging to the main scripts folder (adjust if scripts go elsewhere in Composer)
                    gsutil -m cp -r gs://${GCS_BUCKET_NAME}/staging/scripts/* gs://${GCS_BUCKET_NAME}/scripts/
                    """
                }
            }
        }
    }
}
