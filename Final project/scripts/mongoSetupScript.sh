#Create a /etc/yum.repos.d/mongodb-org-4.0.repo file to install MongoDB using yum
sudo touch /etc/yum.repos.d/mongodb-org-4.0.repo

#write following into the file created above
sudo bash -c 'echo "[mongodb-org-4.0]" > /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'echo "name=MongoDB Repository" >> /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'echo "baseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/4.0/x86_64/" >> /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'echo "gpgcheck=1" >> /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'echo "enabled=1" >> /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'echo "gpgkey=https://www.mongodb.org/static/pgp/server-4.0.asc" >> /etc/yum.repos.d/mongodb-org-4.0.repo'
sudo bash -c 'cat /etc/yum.repos.d/mongodb-org-4.0.repo'

#install mongoldb packages using yum
sudo yum install -y mongodb-org

#start mongoldb service
sudo service mongod start