## Image Tagging Script:
```bash
./image_tagging_script.sh -f src/Dockerfile --token-file token-file.txt --username git git.tu-berlin.de:5000/zodiac/zodiac-meta/mcp-server src/zodiac-mcp-client/deployment-client.yaml 
```

## Build and deploy:
# Build and tag manually for the first deploy
docker buildx build --platform linux/amd64 -t repo/stream-manager:latest .
docker push repo

# Then deploy to the cluster
kubectl apply -f k8s/stream-manager-deployment.yaml


