# Problemas

Ciclo limitado a 100 000 iterações cujo tempo de espera num poll() é de 1 segundo, o que leva a que um único processo destes corra limpinho durante no máximo 100.000s, forçando reiniciar depois de detetar que o processo já não está a correr.

O consumer group do lado do data engineering, está a dar commit à leitura de uma mensagem independentemente de o evento ter sido inserido na bases de dados com sucesso.

Só temos um consumidor por consumer group, e nada nos impede de termos mais

A estrutura existente de kafka permite qualquer pessoa ser um consumidor de kafka desde que tenha o ip da maquina.

Consumir por lista de mensagens ao invés de uma por uma

Adicionar threads para a deserialização das mensagens. Para tal podemos dividir a lista em conjuntos de generators. Podemos ter threads independentes a fazer este trabalho.

stream_rows esta a fazer demasiados pedidos por chamada





# Propostas

**Qualquer uma das propostas feitas devem cumprir com os seguintes requisitos:**
- Reliable: Deve ser capaz de garantir que cada evento é inserido pelo menos uma vez na tabela do bigquery
- Escalável: Deve conseguir escalar por forma a consumir os eventos em paralelo num único grupo de consumidores.
- Real-Time: O end-to-end delay entre produção de uma mensagem e consumo da mesma deve ser inferior a 10 segundos.
- Monitorização: 
	- Deve haver uma estrutura de monitorização montada para melhor entender o fluxo das mensagens, tanto ao nível de quantidade de mensagens a serem produzidas, como também a quantidade de mensagens a serem consumidas. 
	- Analisando o consumo de mensagens, isto deve ser feito ao nível de um grupo de consumidores.

## Consumidor python

### Real-time

Iterar num ciclo infinito:
- Menos overhead de inicialização do processo.

Aumentar a quantidade de consumidores por grupo de consumidores:
- Para tal, temos de aumentar a quantidade de partições que um tópico 

Digerir as mensagens em batch: 
- Para um consumidor python há um pequeno catch: a função poll() nativa do kafka pode fazer o fetch das mensagens em batch e vai guardá-las em memoria, mas passa as mensagens para o python um por um. Para termos esta funcionalidade temos nos de produzir um tipo de batch no lado do python. Cada evento que recebemos podemos guardar numa lista python, e quando exceder um determinado valor ou uma determinada quantidade de tempo, processamos a lista de eventos e populamos de uma só vez no bigquery.
- https://stackoverflow.com/questions/45920608/how-to-read-batch-messages-in-confluent-kafka-python/45932153


### Reliability

Dar commit à mensagem depois de termos a certeza de que conseguimos inserir os dados na base de dados.


### Escalável

Podemos construir isto como um deployment do kubernetes que nos daria acesso à escalabilidade dos consumidores com base numa métrica.

A quantidade de consumidores que podemos ter estaria limitado ao numero de partições que o tópico tem, 

A métrica que define quantos pods o kubernetes deve correr para conseguirmos escalar os pods deve ser a quantidade de mensagens que 

### Monitorização



## Consumidor Kafka Connect

Um grupo de consumidores orientado a inserir os dados de um (ou mais) topico para a tabela do bigquery (datalake)
