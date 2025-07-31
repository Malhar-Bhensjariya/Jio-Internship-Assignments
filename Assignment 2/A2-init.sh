echo "Creating Unmanaged Instance Group for Primary Instance Group.........."
gcloud compute instance-groups unmanaged create $PRIMARY_INSTANCE_GROUP --zone=$ZONE
gcloud compute instance-groups unmanaged add-instances $PRIMARY_INSTANCE_GROUP --instances=$ACTIVE_VM --zone=$ZONE

echo "Creating Unmanaged Instance Group for Standby Instance Group.........."
gcloud compute instance-groups unmanaged create $SECONDARY_INSTANCE_GROUP --zone=$ZONE
gcloud compute instance-groups unmanaged add-instances $SECONDARY_INSTANCE_GROUP --instances=$STANDBY_VM --zone=$ZONE

echo "Creating Backend Service (Internal)..........."
gcloud compute backend-services create $BACKEND_SERVICE \
    --load-balancing-scheme=INTERNAL \
    --protocol=TCP \
  	--health-checks=$HEALTH_CHECK \
  	--region=$REGION

echo "Adding Primary Backend (VM2)..........."
gcloud compute backend-services add-backend $BACKEND_SERVICE \
--instance-group=$PRIMARY_INSTANCE_GROUP \
--instance-group-zone=$ZONE \
--balancing-mode=CONNECTION \
--region=$REGION

echo "Adding Standby Backend (VM3) with Failover..........."
gcloud compute backend-services add-backend $BACKEND_SERVICE \
--instance-group=$SECONDARY_INSTANCE_GROUP \
--instance-group-zone=$ZONE \
--balancing-mode=CONNECTION \
--failover \
--region=$REGION

echo "Creating Forwarding Rule (Internal)..........."
gcloud compute forwarding-rules create $FORWARDING_RULE \
--load-balancing-scheme=INTERNAL \
--backend-service=$BACKEND_SERVICE \
--ip-protocol=TCP \
--ports=81 \
--subnet=$SUBNET \
--region=$REGION


echo "INTERNAL IP Address..........."
gcloud compute forwarding-rules describe $FORWARDING_RULE --region=$REGION --format="get(IPAddress)"

echo "Creating Routes..........."
gcloud compute routes create route-vm2 \
--network=$NETWORK \
--destination-range=0.0.0.0/0 \
--next-hop-instance=lbtcp-vm2 \
--next-hop-instance-zone=$ZONE \
--priority=100

gcloud compute routes create route-vm3 \
--network=$NETWORK \
--destination-range=0.0.0.0/0 \
--next-hop-instance=lbtcp-vm3 \
--next-hop-instance-zone=$ZONE \
--priority=200
