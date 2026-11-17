package bbs.client;

import bbs.proto.CreateChannelRequest;
import bbs.proto.Envelope;
import bbs.proto.ListChannelsRequest;
import bbs.proto.LoginRequest;
import bbs.proto.PublishRequest;
import bbs.proto.Status;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Bot (cliente) do BBS - Partes 1 e 2.
 *
 * Dois sockets, cada um com sua função:
 *   REQ -> broker (:5555): login, criar/listar canais e pedir publicação.
 *   SUB -> proxy  (:5558): recebe as mensagens dos canais em que está inscrito.
 *
 * Comportamento padronizado pela especificação (sem nenhuma interação humana):
 *   1. Se existirem menos de 5 canais, cria um novo.
 *   2. Se estiver inscrito em menos de 3 canais, inscreve-se em mais um (aleatório).
 *   3. Loop infinito: escolhe um canal da lista e publica 10 mensagens
 *      aleatórias, com 1 segundo de intervalo entre elas.
 *
 * NOTA: os passos 1 e 2 são reavaliados a cada volta do loop. Assim o bot
 * converge para 5 canais / 3 inscrições mesmo começando do zero.
 * Para executá-los só uma vez, mova as duas chamadas para antes do while em run().
 *
 * Por que uma thread separada para o SUB? Receber é bloqueante e as mensagens
 * chegam a qualquer momento, inclusive enquanto o bot está dormindo entre
 * publicações. Sockets ZeroMQ não são thread-safe: o socket SUB é criado e usado
 * SÓ pela thread assinante; a thread principal pede novas inscrições por uma fila.
 *
 * Variáveis de ambiente: BROKER_ADDR, SUB_ADDR, BOT_NAME, MIN_CHANNELS (5),
 * MIN_SUBSCRIPTIONS (3), BURST_SIZE (10), MESSAGE_INTERVAL_MS (1000).
 */
public class Bot implements AutoCloseable {

    private static final int TIMEOUT_MS = 5000;   // espera máxima por uma resposta
    private static final int MAX_RETRIES = 5;     // tentativas por requisição
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // Nomes candidatos para canais novos.
    private static final List<String> CHANNEL_POOL =
            List.of("geral", "random", "tecnologia", "jogos", "musica", "filmes");
    // Frases para as mensagens aleatórias.
    private static final List<String> PHRASES = List.of(
            "Alguém por aqui?", "Bom dia, pessoal!", "Qual a novidade?", "Testando o canal",
            "Esse BBS é nostálgico", "Quem aí lembra do modem 56k?", "Mensagem de teste",
            "Boa tarde a todos", "Ping!", "Vamos conversar?");

    private final String brokerAddr;
    private final String subAddr;
    private final int minChannels;
    private final int minSubscriptions;
    private final int burstSize;
    private final long messageIntervalMs;
    private final Random random = new Random();

    private final ZContext ctx = new ZContext();
    private ZMQ.Socket socket;                  // REQ (usado só pela thread principal)
    private volatile String username;           // lido também pela thread assinante (logs)

    // Estado compartilhado entre a thread principal e a thread assinante.
    private final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
    private final Queue<String> pendingSubscribes = new ConcurrentLinkedQueue<>();
    private volatile boolean running = true;
    private Thread subscriberThread;

    public Bot() {
        this.brokerAddr = env("BROKER_ADDR", "tcp://broker:5555");
        this.subAddr = env("SUB_ADDR", "tcp://proxy:5558");
        this.username = env("BOT_NAME", "bot-" + Integer.toHexString(random.nextInt(0xFFFF)));
        this.minChannels = Integer.parseInt(env("MIN_CHANNELS", "5"));
        this.minSubscriptions = Integer.parseInt(env("MIN_SUBSCRIPTIONS", "3"));
        this.burstSize = Integer.parseInt(env("BURST_SIZE", "10"));
        this.messageIntervalMs = Long.parseLong(env("MESSAGE_INTERVAL_MS", "1000"));
        connect();
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    // ------------------------------------------------------------------
    // Rede REQ/REP
    // ------------------------------------------------------------------

    /** (Re)cria o socket REQ. Necessário após timeout: um REQ que perdeu a resposta fica "travado". */
    private void connect() {
        if (socket != null) {
            ctx.destroySocket(socket);
        }
        socket = ctx.createSocket(SocketType.REQ);
        socket.setLinger(0);                 // não segurar mensagens pendentes ao fechar
        socket.setReceiveTimeOut(TIMEOUT_MS);
        socket.connect(brokerAddr);
    }

    /**
     * Envia o Envelope e devolve a resposta. Se o servidor não responder a
     * tempo (ex.: ainda subindo), recria o socket e tenta de novo.
     * ATENÇÃO: um reenvio após timeout pode duplicar uma publicação se o
     * servidor chegou a processar o pedido original (aceitável nesta parte).
     */
    private Envelope request(Envelope req) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            log("SEND", req);
            socket.send(req.toByteArray(), 0);
            byte[] data = socket.recv(0);    // null em caso de timeout

            if (data != null) {
                try {
                    Envelope rep = Envelope.parseFrom(data);
                    log("RECV", rep);
                    return rep;
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("resposta inválida do servidor", e);
                }
            }
            note("sem resposta (tentativa " + attempt + "/" + MAX_RETRIES + "), reconectando...");
            connect();
        }
        throw new IllegalStateException("servidor indisponível");
    }

    /** Cria um Envelope já com o timestamp do envio (campo obrigatório). */
    private static Envelope.Builder envelope() {
        return Envelope.newBuilder().setTimestamp(System.currentTimeMillis());
    }

    // ------------------------------------------------------------------
    // Logs
    // ------------------------------------------------------------------

    private static String clock(long epochMillis) {
        return LocalTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault()).format(CLOCK);
    }

    /** Valor legível de um campo (enum vira o nome, ex.: OK). */
    private static String fmt(Object value) {
        return value instanceof Descriptors.EnumValueDescriptor e ? e.getName() : value.toString();
    }

    private static Descriptors.FieldDescriptor activeField(Envelope env) {
        Descriptors.OneofDescriptor oneof = env.getDescriptorForType().getOneofs().get(0);
        return env.getOneofFieldDescriptor(oneof);
    }

    /**
     * Lista TODOS os campos da mensagem como chave=valor, inclusive os de valor
     * padrão (ex.: status=OK, que o formato texto do protobuf omitiria).
     * Strings vazias (ex.: error sem erro) são omitidas. Usa os descritores,
     * então funciona para qualquer mensagem nova adicionada ao .proto.
     */
    private static String describeBody(Envelope env) {
        Descriptors.FieldDescriptor active = activeField(env);
        if (active == null) {
            return "";
        }
        Message body = (Message) env.getField(active);
        List<String> parts = new ArrayList<>();
        for (Descriptors.FieldDescriptor f : body.getDescriptorForType().getFields()) {
            Object value = body.getField(f);
            if (f.isRepeated()) {
                String items = ((List<?>) value).stream().map(Bot::fmt).collect(Collectors.joining(", "));
                parts.add(f.getName() + "=[" + items + "]");
            } else if (f.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING
                    && ((String) value).isEmpty()) {
                continue;
            } else {
                parts.add(f.getName() + "=" + fmt(value));
            }
        }
        return String.join(" ", parts);
    }

    /**
     * Exibe cada mensagem enviada/recebida com todo o conteúdo (exigência da spec):
     *   HH:MM:SS.mmm | origem | SEND/RECV | tipo | campos...  ts=<hora do envio>
     * "suffix" permite acrescentar informação extra (ex.: hora do recebimento).
     */
    private void log(String direction, Envelope env, String suffix) {
        Descriptors.FieldDescriptor active = activeField(env);
        String kind = active == null ? "(vazio)" : active.getName();
        String body = describeBody(env);
        String content = (body.isEmpty() ? "" : body + "  ") + "ts=" + clock(env.getTimestamp()) + suffix;
        System.out.printf("%s | %-10s | %-4s | %-19s | %s%n",
                LocalTime.now().format(CLOCK), username, direction, kind, content);
    }

    private void log(String direction, Envelope env) {
        log(direction, env, "");
    }

    /** Mensagem informativa do bot (não é troca de mensagens com o servidor). */
    private void note(String text) {
        System.out.printf("%s | %-10s | INFO | %s%n", LocalTime.now().format(CLOCK), username, text);
    }

    // ------------------------------------------------------------------
    // Operações do protocolo (REQ/REP)
    // ------------------------------------------------------------------

    /** Login: uma mensagem de ida, uma de volta. Se der erro, repete com outro nome. */
    public void login() throws InterruptedException {
        String baseName = username;
        while (true) {
            Envelope rep = request(envelope()
                    .setLoginReq(LoginRequest.newBuilder().setUsername(username))
                    .build());

            if (rep.getBodyCase() == Envelope.BodyCase.LOGIN_REP
                    && rep.getLoginRep().getStatus() == Status.OK) {
                return;
            }
            // Erro: tenta de novo com um sufixo aleatório.
            username = baseName + "-" + Integer.toHexString(random.nextInt(0xFFFF));
            Thread.sleep(2000);
        }
    }

    /** Pede ao servidor a lista com o nome de todos os canais existentes. */
    public List<String> listChannels() {
        Envelope rep = request(envelope()
                .setListChannelsReq(ListChannelsRequest.getDefaultInstance())
                .build());
        return new ArrayList<>(rep.getListChannelsRep().getChannelsList());
    }

    /** Pede a criação de um canal. Retorna true se o servidor respondeu OK. */
    public boolean createChannel(String channel) {
        Envelope rep = request(envelope()
                .setCreateChannelReq(CreateChannelRequest.newBuilder()
                        .setChannel(channel)
                        .setUsername(username))
                .build());
        return rep.getBodyCase() == Envelope.BodyCase.CREATE_CHANNEL_REP
                && rep.getCreateChannelRep().getStatus() == Status.OK;
    }

    /** Pede ao servidor para publicar um texto em um canal. Retorna true se OK. */
    public boolean publish(String channel, String text) {
        Envelope rep = request(envelope()
                .setPublishReq(PublishRequest.newBuilder()
                        .setChannel(channel)
                        .setUsername(username)
                        .setText(text))
                .build());
        return rep.getBodyCase() == Envelope.BodyCase.PUBLISH_REP
                && rep.getPublishRep().getStatus() == Status.OK;
    }

    // ------------------------------------------------------------------
    // Assinatura de canais (SUB) - roda em thread própria
    // ------------------------------------------------------------------

    /** Inscreve-se em um canal. A inscrição de fato é feita pela thread assinante. */
    private void subscribe(String channel) {
        if (subscriptions.add(channel)) {
            pendingSubscribes.add(channel);
            note("inscrito no canal '" + channel + "' (" + subscriptions.size() + " inscrição(ões))");
        }
    }

    private void startSubscriber() {
        subscriberThread = new Thread(this::subscriberLoop, "subscriber");
        subscriberThread.setDaemon(true);
        subscriberThread.start();
    }

    private void subscriberLoop() {
        // Este socket pertence exclusivamente a esta thread.
        ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
        sub.setReceiveTimeOut(200);          // acorda a cada 200 ms para ver novas inscrições
        sub.connect(subAddr);

        try {
            while (running) {
                // 1) aplica as inscrições pedidas pela thread principal (todas na MESMA conexão)
                String channel;
                while ((channel = pendingSubscribes.poll()) != null) {
                    sub.subscribe(channel.getBytes(StandardCharsets.UTF_8));
                }

                // 2) recebe uma publicação: frame 1 = tópico, frame 2 = Envelope
                String topic = sub.recvStr();
                if (topic == null || !sub.hasReceiveMore()) {
                    continue;                // timeout (nada chegou) ou mensagem malformada
                }
                byte[] payload = sub.recv(0);
                handlePublication(topic, payload);
            }
        } catch (RuntimeException e) {
            if (running) {
                note("thread assinante encerrada por erro: " + e);
            }
        }
    }

    private void handlePublication(String topic, byte[] payload) {
        try {
            Envelope env = Envelope.parseFrom(payload);
            if (env.getBodyCase() != Envelope.BodyCase.CHANNEL_MSG) {
                return;
            }
            String channel = env.getChannelMsg().getChannel();
            // O filtro do ZeroMQ casa por PREFIXO: quem assinou "geral" também receberia
            // "geral2". Por isso conferimos o nome exato do canal.
            if (!topic.equals(channel) || !subscriptions.contains(channel)) {
                return;
            }
            // Exibe canal, mensagem, timestamp do envio (ts=) e do recebimento (received=).
            log("RECV", env, "  received=" + LocalTime.now().format(CLOCK));
        } catch (InvalidProtocolBufferException e) {
            note("publicação inválida ignorada no tópico '" + topic + "'");
        }
    }

    // ------------------------------------------------------------------
    // Comportamento do bot
    // ------------------------------------------------------------------

    /** Passo 1: se há menos de minChannels canais, cria um com nome ainda livre. */
    private void ensureChannels(List<String> existing) {
        if (existing.size() >= minChannels) {
            return;
        }
        List<String> candidates = new ArrayList<>(CHANNEL_POOL);
        candidates.removeIf(c -> existing.stream().anyMatch(c::equalsIgnoreCase));
        String name = candidates.isEmpty()
                ? "canal-" + Integer.toHexString(random.nextInt(0xFFFFF))
                : candidates.get(random.nextInt(candidates.size()));
        createChannel(name);   // se outro bot criou antes, o servidor responde "já existe": sem problema
    }

    /** Passo 2: se está inscrito em menos de minSubscriptions canais, inscreve-se em mais um aleatório. */
    private void ensureSubscriptions(List<String> channels) {
        if (subscriptions.size() >= minSubscriptions) {
            return;
        }
        List<String> candidates = new ArrayList<>(channels);
        candidates.removeIf(subscriptions::contains);
        if (!candidates.isEmpty()) {
            subscribe(candidates.get(random.nextInt(candidates.size())));
        }
    }

    private String randomText() {
        return PHRASES.get(random.nextInt(PHRASES.size())) + " [" + Integer.toHexString(random.nextInt(0xFFFF)) + "]";
    }

    public void run() throws InterruptedException {
        login();
        startSubscriber();

        while (true) {
            List<String> channels = listChannels();
            if (channels.size() < minChannels) {
                ensureChannels(channels);
                channels = listChannels();   // atualiza a lista com o canal recém-criado
            }
            ensureSubscriptions(channels);

            if (channels.isEmpty()) {
                Thread.sleep(1000);
                continue;
            }

            // Passos 4 e 5: escolhe um canal e envia burstSize mensagens, 1 por segundo.
            String target = channels.get(random.nextInt(channels.size()));
            for (int i = 0; i < burstSize; i++) {
                if (!publish(target, randomText())) {
                    note("publicação em '" + target + "' falhou");
                }
                Thread.sleep(messageIntervalMs);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (subscriberThread != null) {
            try {
                subscriberThread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        ctx.close();
    }

    // ------------------------------------------------------------------
    public static void main(String[] args) throws InterruptedException {
        try (Bot bot = new Bot()) {
            bot.run();
        }
    }
}
