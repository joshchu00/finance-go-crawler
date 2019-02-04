node {

  docker.withTool('docker-latest') {

    def image

    stage('Git Pull') {
      git url: 'https://github.com/joshchu00/finance-go-crawler.git', branch: 'develop'
    }
    stage('Go Build') {
      sh "${tool name: 'go-1.11', type: 'go'}/bin/go build -a -o main"
    }
    stage('Docker Build') {
      image = docker.build('joshchu00/finance-go-crawler')
    }
    stage('Docker Push') {
      docker.withRegistry('', 'DockerHub') {
        image.push()
      }
    }
  }
}
