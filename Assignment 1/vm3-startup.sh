#!/bin/bash
# Update & install Docker
apt update -y
apt install -y docker.io

# Start and enable Docker
systemctl start docker
systemctl enable docker

# Wait a bit for Docker to be ready
sleep 5

# Create working directory
mkdir -p /test && cd /test

# Create custom nginx.conf that listens on port 81
cat <<EOF > nginx.conf
server {
    listen 81;
    server_name localhost;

    location / {
        root /usr/share/nginx/html;
        index index.html;
    }
}
EOF

# Create HTML page
cat <<EOF > index.html
<h1>Server VM3 - Port 81</h1>
EOF

# Create Dockerfile
cat <<EOF > Dockerfile
FROM nginx:alpine
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
EXPOSE 81
EOF

# Build and run the Docker container
docker build -t clean-nginx . >> /var/log/docker-build.log 2>&1
docker run -d -p 81:81 clean-nginx >> /var/log/docker-run.log 2>&1
