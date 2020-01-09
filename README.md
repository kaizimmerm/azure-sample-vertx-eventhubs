# Eclipse Vert.x sample for Azure Event Hubs

This sample code demonstrates various features of the [Eclipse Vert.x Kafka client](https://vertx.io/docs/vertx-kafka-client/java/) connected to [Azure Event Hubs' Kafka API](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview).

The client is configured by [Spring Boot properties](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config).

## Prerequisites to run the sample

- An [Azure subscription](https://azure.microsoft.com/en-us/get-started/).
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) installed to setup the infrastructure.
- Java 11 and Maven installed

## HowTo setup the sample

The sample expects to run on two Event Hubs/Kafka Topics "test" and "test2" with 8 partitions each. We can leverage an [ARM template](https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/overview) to create the necessary infrastructure for us.

Set parameters:

```bash
resourcegroupName=eh-sample
location=westeurope
namespaceName=my-eh-sample-ns
```

Create infrastructure:

```bash
az group create --name $resourcegroupName --location $location

az group deployment create --name vertxEventHubsSampleInfrastructure --resource-group $resourcegroupName --template-file azureDeploy.json --parameters namespaceName=$namespaceName

eh_connection=`az group deployment show --name vertxEventHubsSampleInfrastructure --resource-group $resourcegroupName --query properties.outputs.namespaceConnectionString.value -o tsv`
```

Run code:

```bash
mvn verify -Dkaizimmerm.test.eh.namespace=$namespaceName -Dkaizimmerm.test.eh.connection-string=$eh_connection
```

## CleanUp Azure resources

```bash
az group delete --name $resourcegroupName
```
