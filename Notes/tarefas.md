*1 dia = 8 horas*

*1 semana = 3 dias*

### Consolidação do levantamento
- Organizar as notas de: 
	- Bigquery
	- Kafka
	- Kubernetes

(1-3 dias)

- Notas sobre a arquitetura global da HUUB 
- Notas sobre a arquitetura ETL

_**Main goal:**_ State of the art - Huub actual arquitetura 

(1-3 dias)

- Definição dos objetivos: 
	- Compliance com microserviços
	- Near real-time
	- Reliability
	- Monitorizar

(1 dia)

**(Total 3-7 dias)**

### Consumidor a Solo
- Realização de testes relativo ao consumo de mensagens do consumidor atual
- Apontar melhorias que podem ser realizadas no consumidor atual + testes

(1-2 dias)

- Comparações entre o kafka connect e o consumidor do ETL

(1 dia)

**(Total 2-3 dias)**


### Análise Estatistica
- Através da máxima latência, desenho da estrutura kafka que não exceda este valor no pior cenário
	- Definir métricas para serem captadas; 
	- Fazer o desenho da arquitetura para ser complient com os objetivos

(1 semana)

- Medição da latência média e máxima:
	- Usando a arquitetura atual?

(1-2 dias)

**(Total 4-5 dias)**


### Que ferramentas permitem escalabilidade
- Que ferramentas dão acesso a uma escalabilidade horizontal?

(1 dia)

- Desenho de uma arquitetura que permita a escalabilidade autónoma de um consumidor
	- definição de parâmetros para definir quando se dá a "escalagem"
	- desenho do processo que correrá em paralelo em cada maquina

(1 semana)

**(Total 4 dias)**


### Que configurações permitem reliability 
- Reliability do lado de um broker (produção de mensagem)
		- liveness, readiness, replicas de partições
- Reliability do lado de um consumidor (garantir que uma mensagem só é dada como lida, após ter sido dada como inserida no big query)

(2 dias)

**(Total 2 dias)**

### Monitorização
- Definição do que se quer monitorizar

(1-2 dias)

- Algum serviço já faz a recolha desses dados? (Kafka, Kubernetes, ...)

(1 dia)

- Queremos criar um tipo de microserviço que com base nos dados que recolhe lança alarmes ou avisos?

(0 dias : 1-2 semanas)

**(Total 2-9 dias)**




### Conclusão
- Fundamentar a arquitetura para os diversos sistemas: 
	- Sistema de consumo
	- Tópicos e partições

(1 semana)

- Comparação dos diferentes sistemas atual e desenhado através das seguintes variáveis:
	- Custo
	- Tempo de Consumo

(1-2 semanas)

**(Total 6-9 dias)**

