## Duvidas Data Engineering

Q: Qual o programa que inicializa estes processos, de maneira a perceber quando estes processos vão abaixo ou não?

Q: Pq é que se entra num ciclo limitado a 100000 iterações em vez de fazermos um while True? (kafka_consumer.Command.handle())

Q: O facto de estarmos a dar commit da mensagem sem sabermos se foi inserida com sucesso ou não, não torna o serviço unreliable?

Q: pq e que o delivery_changed_handler.handle() não tem o conn.commit() e o conn.execute() dentro de um try, except? Isto não leva a perda de dados no caso de nao se conseguir inserir por algum motivo?

Q: No contexto do delivery_changed_handler.hande():
Há um if else block que tem duas formas de inserir os dados na base de dados.
Um deles usa o bigQueryHelper, outro usa o sqlalchemy.
No final deste metodo, o erro só é visto no caso do bigQueryHelper nao conseguir inserir os dados e não no caso do sqlalchemy ter esse mesmo problema... 



## Duvidas Backend

Q: Qualquer pessoa que se consiga connectar ao bootstrap_server consegue consumir as mensagens? Ou há algum mecanismo de segurança?

Q: type_from_full_name, exemplo de uma string item_type_name para perceber de que modulo so importados os item_types.

Q: Em que módulo estão os item_types que são importados pelo método acima?

Q: Um topico tem diferentes serializers que são escolhidos com base no header do record item_type_name. Isto implica que o record que vai para esse topico tem diferentes estruturas possíveis, ou todas as mensagens para esse topico têm a mesma estrutura?
No caso de terem todos a mesma estrutura para aquele topico, para que serve ter varios serializers?

Q: Seria possível ver um record do DeliveryEvents?

