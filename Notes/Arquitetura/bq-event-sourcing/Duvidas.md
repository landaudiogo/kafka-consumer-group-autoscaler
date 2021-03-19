Q: Onde está o modulo config do ficheiro settings.py?  (delivery_to_bigquery)

Q: Pq é que o bigquery eventsourcing só faz subscribe ao topico do DeliveryEventsV6Topic

Q: Pq é que se entra num ciclo limitado a 100000 iterações em vez de fazermos um while True? (kafka_consumer.Command.handle())

Q: Quando inicializamos o Command o poll_timeout é chamado. Para que serve este atributo?
	1. Chamamos poll() e depois de poll_timeout saímos de poll?
	2. Chamamos poll() e depois de receber dados, esperamos poll_timeout para sair do poll()? Ou seja seria esperamos poll_timeout para recebermos mais informação que o topico possa ter para o método.
	
	
Q: type_from_full_name, exemplo de uma string item_type_name para perceber de que modulo so importados os item_types.

Q: pq e que o delivery_changed_handler.handle() não tem o conn.commit() e o conn.execute() dentro de um try, except? Isto não leva a perda de dados no caso de nao se conseguir inserir por algum motivo?

Q: No contexto do delivery_changed_handler.hande():
Há um if else block que tem duas formas de inserir os dados na base de dados.
Um deles usa o bigQueryHelper, outro usa o sqlalchemy.
No final deste metodo, o erro só é visto no caso do bigQueryHelper nao conseguir inserir os dados e não no caso do sqlalchemy ter esse mesmo problema... 


