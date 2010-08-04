#!/usr/bin/perl
use 5.10.0;
use strict;
use warnings;
use DBI;

my $dbi = DBI->connect('dbi:Pg:dbname=rtdb;host=172.30.116.2',
                       'ruoso', 'devel', { pg_enable_utf8 => 1 });

my %nomes_filas;
{   # vamos montar um hash com os ids das filas...
    my $sth_queues = $dbi->prepare('SELECT id, name FROM queues') || die $dbi->errstr;
    $sth_queues->execute();
    while (my ($id, $nome) = $sth_queues->fetchrow_array) {
        $nomes_filas{$nome} = $id;
    }
};
my %ids_filas = reverse %nomes_filas;

my $id_minimo = 421;
my $id_maximo = 421;
my $data_minima = '-infinity';
my $data_maxima = 'infinity';

my $sth_historico_completo = $dbi->prepare
  ('SELECT * FROM transactions WHERE '.
   'objecttype=? AND objectid=? '.
   'order by created DESC');


{   # vamos pasar por todos os tickets.
    # precisamos passar por todos, porque não sabemos se ele, em algum
    # momento passou pela fila que interessa à gente. Então, para
    # simplificar, vamos passar por todos e analisar o histórico de
    # todos.

    my $sth_tickets = $dbi->prepare
      ('SELECT * FROM tickets WHERE '.
       'created BETWEEN ? AND ? AND '.
       'id BETWEEN ? AND ? '.
       'ORDER BY id');
    $sth_tickets->execute($data_minima, $data_maxima,
                          $id_minimo, $id_maximo);
    my $sth_historico = $dbi->prepare
      ('SELECT oldvalue FROM transactions WHERE '.
       'objecttype=? AND objectid=? AND '.
       'field=? '.
       'ORDER BY created');

  TICKET:
    while (my $ticket = $sth_tickets->fetchrow_hashref) {
        # Agora eu vou ver se esse ticket: 1) Tem no atributo queue
        # uma fila que interessa, ou 2) Tem no histórico a mudança de
        # fila com alguma fila que interessa.

        if (fila_interessa($ticket->{queue})) {
            processar_ticket_suporte($ticket);
        } else {
            $sth_historico->execute('RT::Ticket', $ticket->{id}, 'Queue') || die $dbi->errstr;
          HISTORICO:
            while (my ($oldqueue) = $sth_historico->fetchrow_array) {
                if (fila_interessa($oldqueue)) {
                    processar_ticket_suporte($ticket);
                    last HISTORICO;
                }
            }
        }
    }

};

sub fila_interessa {
    my $queue_id = shift;
    return 1 if
      $ids_filas{$queue_id} =~ /cos/i &&
        $ids_filas{$queue_id} =~ /1/i;
    return 0;
}


sub processar_ticket_suporte {
    my $ticket = shift;
    my @historico;
    $sth_historico_completo->execute('RT::Ticket', $ticket->{id});
    while (my $t = $sth_historico_completo->fetchrow_hashref) {
        push @historico, $t;
    }

    # vamos pegar as alterações que são feitas todas de uma vez e
    # consolidar (usando o campo created);
    my @historico_consolidado;
    my $alteracoes = {};
    my $last_created = 0;
    foreach my $t (@historico) {
        if ($last_created ne $t->{created}) {
            push @historico_consolidado,
              { created => $last_created,
                alteracoes => $alteracoes };
            $alteracoes = {};
            $last_created = $t->{created};
        }
        $alteracoes->{lc($t->{field})} = $t->{oldvalue};
    }

    # Agora vamos fazer uma lista consolidada das versões dos tickets
    # incluindo o periodo quando ele foi valido.
    my $working_ticket = $ticket;
    my @versoes = ( $ticket ); # o primeiro elemento é a versão recente
    foreach my $patch (@historico_consolidado) {
        $working_ticket->{transaction_time} = $patch->{created};
        $working_ticket = { %$working_ticket, %{$patch->{alteracoes}} };
        push @versoes, $working_ticket;
    }
    @versoes = reverse @versoes;


    # Processo de Suporte
    # Ticket chega no primeiro nivel como "new" e sem proprietario
    # Alguém toma o ticket e abre "open".
    # Pode ser resolvido no primeiro nivel.
    # Ou então repassa para outra fila (2o. nivel)
    # Alguém toma o ticket no segundo nivel.
    # Pode ser resolvido no segundo nivel.
    # Ou ficar pendente.
    # Então é resolvido no Terceiro Nivel.

    # D datahora_requisicao
    # D datahora_inicio_atendimento
    # D atendente no primeiro nivel
    # D fila_segundo_nivel
    # D datahora_escala_segundo_nivel
    # D datahora_inicio_atendimento_segundo_nivel
    # D atendente no segundo nivel
    # D datahora_escala_terceiro_nivel
    # D datahora_resolucao
    # M minutos_uteis_de_espera_para_inicio
    # M minutos_corridos_de_espera_para_inicio
    # M minutos_uteis_encaminhamento_primeiro_nivel
    # M minutos_corridos_encaminhamento_primeiro_nivel
    # M minutos_uteis_de_espera_segundo_nivel
    # M minutos_corridos_de_espera_segundo_nivel
    # M minutos_uteis_encaminhamento_segundo_nivel
    # M minutos_corridos_encaminhamento_segundo_nivel
    # M minutos_uteis_pendencia_terceiro_nivel
    # M minutos_corridas_pendencia_terceiro_nivel
    # M minutos_uteis_total_atendimento
    # M minutos_corridos_total_atendimento

}
