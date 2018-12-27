#kubectl set up v1.10.0 (newest is 1.13.0)
#download binary
curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.10.0/bin/darwin/amd64/kubectl
#cheange permissions
chmod +x ./kubectl
#move to binary folder
sudo mv ./kubectl /usr/local/bin/kubectl
#check client version of kubectl
kubectl version --client


#minikube setup
#install minikube, docker and virtualbox
brew update && brew cask install docker minikube virtualbox 
#check minikube version (downloads 0.30), docker version
minikube version
docker --version


#start cluster
minikube start


#steps to set up the spark deployment

#create a namespace
kubectl create -f yaml-files/namespace-kbspark-cluster.yaml

#create context for the namespace created above
CURRENT_CONTEXT=$(kubectl config view -o jsonpath='{.current-context}')
USER_NAME=$(kubectl config view -o jsonpath='{.contexts[?(@.name == "'"${CURRENT_CONTEXT}"'")].context.user}')
CLUSTER_NAME=$(kubectl config view -o jsonpath='{.contexts[?(@.name == "'"${CURRENT_CONTEXT}"'")].context.cluster}')
kubectl config set-context spark --namespace=kbspark-cluster --cluster=${CLUSTER_NAME} --user=${USER_NAME}
kubectl config use-context spark


#start master service
kubectl create -f yaml-files/spark-master-controller.yaml
#create service end point
kubectl create -f yaml-files/spark-master-service.yaml
#check master status
kubectl get pods


#Proxy for Spark UI
#proxy controller
kubectl create -f yaml-files/spark-ui-proxy-controller.yaml
#loadbalancer
kubectl create -f yaml-files/spark-ui-proxy-service.yaml
#proxy
kubectl proxy --port=8001


#Spark worker 
#replication controller to manage workers
kubectl create -f yaml-files/spark-worker-controller.yaml
#check status 
kubectl get pods


#Zeppelin UI
#Deploy Zeppelin
kubectl create -f yaml-files/zeppelin-controller.yaml
#Create Service
kubectl create -f yaml-files/zeppelin-service.yaml
#check status
kubectl get pods -l component=zeppelin
#Port forwarding for opening Zeppelin notebook on localhost:8080
kubectl port-forward zeppelin-controller-zl10f 8080:8080
#Open the notebook at localhost:8080 and write the spark code

#Stop cluster
minikube stop

#delete services
minikube delete


#Referenced the official kubernetes website and repository
#https://kubernetes.io/docs/
#https://github.com/kubernetes