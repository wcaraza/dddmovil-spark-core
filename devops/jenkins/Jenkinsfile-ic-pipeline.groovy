@Library('jenkins-sharedlib@master')
import sharedlib.JenkinsfileUtil
def utils = new JenkinsfileUtil(steps,this,'daily')

def project='SHCL'
def recipients="mvillabe@everis.com"

try {

   node {
      stage('Preparation') {
         steps.step([$class: 'WsCleanup', cleanWhenFailure: false])
         utils.notifyByMail('START',recipients)
         checkout scm
         env.project="${project}"
         utils.prepare()
         utils.setGradleVersion("GRADLE481_JAVA8")
      }

      stage('Build & U.Test') {
         utils.buildGradle()
      }

      stage('QA Analisys') {
         //utils.executeSonarWithGradle()
      }

      stage('Results') {
         utils.saveResultGradle('jar')
      }
      
      stage('Upload Artifact') {
         utils.deployArtifactWithGradle()
      }
      
      stage('Post Execution') {
         utils.executePostExecutionTasks()
         utils.notifyByMail('SUCCESS',recipients)
      }
   }
} catch(Exception e) {
   node{
      utils.executeOnErrorExecutionTasks()
      utils.notifyByMail('FAIL',recipients)
    throw e
   }
}
