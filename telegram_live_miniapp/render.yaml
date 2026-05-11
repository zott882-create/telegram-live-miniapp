services:
  - type: web
    name: telegram-live-miniapp
    runtime: docker
    dockerfilePath: ./Dockerfile
    envVars:
      - key: HOST
        value: 0.0.0.0
      - key: PORT
        value: 8080
      - key: COLLECTOR_ENABLED
        value: 1
      - key: LIVE_FROM_DB_ONLY
        value: 1
