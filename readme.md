# Summary

This course is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).


This is a project designed to send data from the openflight network, in doing so it can be used as an exercise for teaching Kusto and focussing on real-time for both Fabric RTA and also Fabric Medallion with real time capabilities such as flight dimensions using inserts and updates of data for dimensions such as time and carrier.

In order to use the sample you'll need to add a username and password from the opensky network to use some more advanced features. For this you'll need to register with the network [here](https://opensky-network.org/). For this sample you can leave the username and password blank if you're only going to get the **state** of each aircraft.

Username and password should be entered as an *AppSetting* or *local.settings.json*.

```
OPEN_SKY_USERNAME
OPEN_SKY_PASSWORD
```

Every 30 secs should yield 10K messages. In order to use this utility create an event hub in Azure ideally with partitions and add the **event hub connection string** and **event hub name**. These should be:

```
EVENT_HUB_CONNECTION_STRING
EVENT_HUB_NAME
```

The following course modules can be undertaken:

1. [Ingesting into Kusto](docs/1.%20Ingesting%20into%20Kusto.md)
2. [Functions, Materialised Views and Update Policies](docs/2.%20Functions,%20Materialised%20Views%20and%20Update%20Policies.md)
3. [Building Kusto Dashboards](docs/3.%20Building%20Kusto%20Dashboards.md)
4. [Building an EventStream with Multiple outputs](docs/4.%20Building%20an%20EventStream%20with%20Multiple%20outputs.md)
5. [Alerting with Data Activator](docs/5.%20Alerting%20with%20Data%20Activator.md)
6. [Reference and Batch with EventHouse](docs/6.%20Reference%20and%20Batch%20with%20EventHouse.md)
7. [Exercise: Medallion with Flight Data](docs/7.%20Exercise:%20Medallion%20with%20Flight%20Data.md)

Happy hunting! Any issues please PR.