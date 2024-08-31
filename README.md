# README

![ARQUITETURA SUGERIDA](./docs/suggested_architecture_kafka.drawio.png)

## Motivador da escolha de uma fila Kafka para pedidos

O Kafka é uma plataforma de streaming de dados em tempo real, na qual os dados são lidos a partir de tópicos e podem ser processados imediatamente. Por isso, ele é ideal para casos em que a ingestão de dados deve ocorrer de forma contínua e com baixa latência.

Uma das principais vantagens do Kafka é sua alta escalabilidade. Podemos adicionar novos produtores, consumidores e até mesmo brokers ao cluster Kafka sem interromper o serviço, facilitando o ajuste da infraestrutura conforme o volume de dados aumenta. No entanto, é fundamental que a squad de pedidos configure o tópico corretamente, gerencie os offsets, e garanta que o consumo dos dados seja eficiente e seguro. Apesar da sua escalabilidade, o Kafka também requer manutenção; se o volume de dados aumentar, é necessário monitorar a infraestrutura, como armazenamento em disco, instâncias de broker e transferência de dados.

Como a principal stack de dados é o Databricks, que possui funcionalidades nativas para leitura de dados de uma fila Kafka, podemos utilizar o Auto Loader para ler os dados de pedidos a partir desses tópicos. Dessa forma, todo o processo de ingestão fica orquestrado e governado pelo Databricks.

Esse processo deve ser configurado diretamente na plataforma, por meio da interface do Delta Live Tables. No repositório, há um código de exemplo para a ingestão dos dados. É importante definir uma política de criação de clusters para otimizar os recursos utilizados e controlar os custos. Além disso, é essencial criar um gatilho de acordo com a necessidade da área de negócio, para que o pipeline seja executado somente quando necessário. Vale lembrar que é preciso apontar o pipeline no Delta Live Tables para um catálogo e um schema, onde as tabelas serão gravadas.
