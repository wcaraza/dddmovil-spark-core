@Library('jenkins-sharedlib@master')
import sharedlib.JenkinsfileUtil
def utils = new JenkinsfileUtil(steps,this,'daily')
def project='SHCL'
def recipients=""
try {
   node {
      stage('Preparation') {
         utils.notifyByMail('START',recipients)
         checkout scm
         env.project="${project}"
         utils.prepare()
         utils.setGradleVersion("GRADLE481_JAVA8")
      }

      stage('Release') {
         utils.promoteReleaseGradle(params.RELEASE_TAG_NAME,"assembleProdRelease")
      }
      
      stage('Results') {
         utils.saveResultGradle('jar')
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