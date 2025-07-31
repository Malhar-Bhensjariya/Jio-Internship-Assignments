echo "Creating VMs........."
gcloud compute instances create lbtcp-vm1 \
--zone=$ZONE \
--machine-type=$MACHINE \
--subnet=$SUBNET \
--image-family=$IMAGE_FAMILY \
--image-project=$IMAGE_PROJECT \
--no-address \
--metadata-from-file startup-script=vm1-startup.sh

gcloud compute instances create lbtcp-vm2 \
--zone=$ZONE \
--machine-type=$MACHINE \
--subnet=$SUBNET \
--tags=$NETWORK_TAG \
--image-family=$IMAGE_FAMILY \
--image-project=$IMAGE_PROJECT \
--no-address \
--metadata-from-file startup-script=vm2-startup.sh

gcloud compute instances create lbtcp-vm3 \
--zone=$ZONE \
--machine-type=$MACHINE \
--subnet=$SUBNET \
--tags=$NETWORK_TAG \
--image-family=$IMAGE_FAMILY \
--image-project=$IMAGE_PROJECT \
--no-address \
--metadata-from-file startup-script=vm3-startup.sh

echo "Creating firewall Rule........."
gcloud compute firewall-rules create $NETWORK_TAG \
--network=$NETWORK \
--allow=tcp:81 \
--source-ranges=10.60.27.0/24,130.211.0.0/22,35.191.0.0/16 \
--target-tags=$NETWORK_TAG

echo "Creating unmanaged group instance............."
gcloud compute instance-groups unmanaged create $INSTANCE_GROUP \
--zone=$ZONE

gcloud compute instance-groups unmanaged add-instances $INSTANCE_GROUP \
--instances=lbtcp-vm2,lbtcp-vm3 \
--zone=$ZONE

gcloud compute instance-groups set-named-ports $INSTANCE_GROUP \
--named-ports=tcp:81 \
--zone=$ZONE

echo "Creating TCP Health Check........."
gcloud compute health-checks create tcp $HEALTH_CHECK \
--port 81

echo "Creating backend services........"
gcloud compute backend-services create $BACKEND_SERVICE \
--load-balancing-scheme=INTERNAL \
--protocol=TCP \
--health-checks=$HEALTH_CHECK \
--region=$REGION

gcloud compute backend-services add-backend $BACKEND_SERVICE \
  --instance-group=$INSTANCE_GROUP \
  --instance-group-zone=$ZONE \
  --region=$REGION

echo "Creating Forwarding Rule..........."
gcloud compute forwarding-rules create $FORWARDING_RULE \
--load-balancing-scheme=INTERNAL \
--ports=81 \
--network=$NETWORK \
--subnet=$SUBNET \
--region=$REGION \
--backend-service=$BACKEND_SERVICE \
--backend-service-region=$REGION \
--ip-protocol=TCP

echo "Checking the status..........."
gcloud compute forwarding-rules describe $FORWARDING_RULE \
  --region=$REGION \
  --format="value(IPAddress)"

gcloud compute health-checks describe $HEALTH_CHECK

gcloud compute backend-services describe $BACKEND_SERVICE --region=$REGION

gcloud compute forwarding-rules describe $FORWARDING_RULE --region=$REGION
