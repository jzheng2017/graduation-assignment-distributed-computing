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
            sh '''
              mvn --batch-mode sonar:sonar
            '''
          }
        }
      }
    }
  }
  post {
    always {
      archiveArtifacts artifacts: 'target/*.jar', fingerprint: true
      junit 'target/surefire-reports/*.xml'
    }
  }
}