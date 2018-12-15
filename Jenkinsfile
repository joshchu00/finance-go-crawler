node {
  def crawlerImage

  stage('Git Pull') {
    git url: 'https://github.com/joshchu00/finance-go-crawler.git', branch: 'develop'
  }
  stage('Go Build') {
    sh "${tool name: 'go-1.11', type: 'go'}/bin/go build -a -o main"
  }
  stage('Docker Build') {
    docker.withTool('docker-latest') {
      crawlerImage = docker.build('docker.io/joshchu00/finance-go-crawler')
    }
  }
  stage('Docker Push') {
    docker.withTool('docker-latest') {
      docker.withRegistry('', 'DockerHub') {
        crawlerImage.push()
      }
    }
  }
}
