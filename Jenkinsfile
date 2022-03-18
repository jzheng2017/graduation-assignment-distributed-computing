pipeline {
  options {
    disableConcurrentBuilds()
    timeout(time: 1, unit: 'HOURS')
  }
  agent {
    kubernetes {
      label "graduation-assignment-jiankai-${UUID.randomUUID().toString()}"
      defaultContainer 'jnlp'
      nodeSelector 'nodegroup=build-nodes'
      yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: maven
    image: maven:3.8.4-openjdk-17
    command:
    - cat
    resources:
      requests:
        memory: '8Gi'
      limits:
        memory: '8Gi'
    tty: true
    tty: true
"""
    }
  }
  stages {
    stage('checkout') {
      steps {
        checkout scm
      }
    }
    stage('build maven') {
      steps {
        container('maven') {
          sh '''
             mvn --batch-mode clean package
          '''
        }
      }
    }
    stage('sonarqube') {
      steps {
        container('maven') {
          withSonarQubeEnv('Luminis SonarQube') {
            sh '''mvn clean verify sonar:sonar -Dsonar.projectKey=graduation-assignment-jiankai'''
          }
        }
      }
    }
    stage("sonar quality gate") {
        steps {
            timeout(time: 2, unit: 'MINUTES') {
                script {
                    def qg = waitForQualityGate() // Reuse taskId previously collected by withSonarQubeEnv
                    if (qg.status != 'OK') {
                        if (env.BRANCH_NAME == 'master' || env.BRANCH_NAME.startsWith('PR-')) {
                            office365ConnectorSend color: "4B9FD5", status: qg.status, message: "DID NOT PASS SONAR QUALITY GATE : ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)", webhookUrl: "myWebhookUrl"
                            error "Pipeline aborted due to Sonar quality gate failure: ${qg.status}"
                        }
                    }
                }
            }
        }
    }
  }
}
