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
import java.util.Random;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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
    private ZMQ.Socket socket;                  // pedidos ao servidor (via broker)
    private final ZMQ.Socket sub;               // publicações dos canais (via proxy)
    private String username;
    private long logicalClock = 0;               // relógio lógico
    private final Set<String> subscriptions = new HashSet<>();   // canais em que estamos inscritos

    public Bot() {
        this.brokerAddr = env("BROKER_ADDR", "tcp://broker:5555");
        this.subAddr = env("SUB_ADDR", "tcp://proxy:5558");
        this.username = env("BOT_NAME", "bot-" + Integer.toHexString(random.nextInt(0xFFFF)));
        this.minChannels = Integer.parseInt(env("MIN_CHANNELS", "5"));
        this.minSubscriptions = Integer.parseInt(env("MIN_SUBSCRIPTIONS", "3"));
        this.burstSize = Integer.parseInt(env("BURST_SIZE", "10"));
        this.messageIntervalMs = Long.parseLong(env("MESSAGE_INTERVAL_MS", "1000"));
        connect();

        sub = ctx.createSocket(SocketType.SUB);
        sub.connect(subAddr);   // as inscrições são feitas depois, canal a canal
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }

    // ------------------------------------------------------------------
    // Rede REQ/REP
    // ------------------------------------------------------------------

    /** recria o socket REQ, que é preciso dps de um timeout **/
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
     * envia o envelope e devolve a resposta. se o servidor não responder a tempo, recria o socket e tenta de novo
     * como o servidor trata login e criação de canal de forma segura para repetição, reenviar não causa dano
     */
    private Envelope request(Envelope req) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            // Carimba NO MOMENTO DO ENVIO: hora e relógio lógico incrementado antes de enviar.
            Envelope msg = req.toBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setLogicalClock(++logicalClock)
                    .build();
            log("SEND", msg);
            socket.send(msg.toByteArray(), 0);
            byte[] data = socket.recv(0);    // null em caso de timeout

            if (data != null) {
                try {
                    Envelope rep = Envelope.parseFrom(data);
                    observeClock(rep.getLogicalClock());   // máximo entre o recebido e o atual
                    log("RECV", rep, "  local_lc=" + logicalClock);
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

    /** Cria o Envelope, timestamp e relógio lógico são preenchidos em request(), no momento do envio */
    private static Envelope.Builder envelope() {
        return Envelope.newBuilder();
    }

    /** Regra 2 de Lamport: novo valor = máximo entre o recebido na mensagem e o atual */
    private void observeClock(long received) {
        logicalClock = Math.max(logicalClock, received);
    }

    // ------------------------------------------------------------------
    // Logs
    // ------------------------------------------------------------------

    private static String clock(long epochMillis) {
        return LocalTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault()).format(CLOCK);
    }

    /** Valor legível de um campo (enum vira o nome, ex.: OK) */
    private static String fmt(Object value) {
        return value instanceof Descriptors.EnumValueDescriptor e ? e.getName() : value.toString();
    }

    private static Descriptors.FieldDescriptor activeField(Envelope env) {
        Descriptors.OneofDescriptor oneof = env.getDescriptorForType().getOneofs().get(0);
        return env.getOneofFieldDescriptor(oneof);
    }

    /**
     * Lista todos os campos da mensagem como chave=valor, inclusive os de valor padrão
     * Ignora strings vazias, par isso, usamos os descritores, então funciona para qualquer mensagem nova adicionada ao .proto */
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

    private void log(String direction, Envelope env, String suffix) {
        Descriptors.FieldDescriptor active = activeField(env);
        String kind = active == null ? "(vazio)" : active.getName();
        String body = describeBody(env);
        String content = (body.isEmpty() ? "" : body + "  ") + "ts=" + clock(env.getTimestamp())
                + "  lc=" + env.getLogicalClock() + suffix;
        System.out.printf("%s | %-10s | %-4s | %-19s | %s%n",
                LocalTime.now().format(CLOCK), username, direction, kind, content);
    }

    private void log(String direction, Envelope env) {
        log(direction, env, "");
    }

    /** Mensagem informativa do bot */
    private void note(String text) {
        System.out.printf("%s | %-10s | INFO | %s%n", LocalTime.now().format(CLOCK), username, text);
    }

    // ------------------------------------------------------------------
    // Operações do protocolo (REQ/REP)
    // ------------------------------------------------------------------

    /** uma mensagem de ida e recebe uma de volta */
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

    /** pede ao servidor a lista com o nome de todos os canais */
    public List<String> listChannels() {
        Envelope rep = request(envelope()
                .setListChannelsReq(ListChannelsRequest.getDefaultInstance())
                .build());
        return new ArrayList<>(rep.getListChannelsRep().getChannelsList());
    }

    /** pede a criação de um canal, retorna true se o servidor respondeu ok */
    public boolean createChannel(String channel) {
        Envelope rep = request(envelope()
                .setCreateChannelReq(CreateChannelRequest.newBuilder()
                        .setChannel(channel)
                        .setUsername(username))
                .build());
        return rep.getBodyCase() == Envelope.BodyCase.CREATE_CHANNEL_REP
                && rep.getCreateChannelRep().getStatus() == Status.OK;
    }

    /** publicação de texto em canais */
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
    // Assinatura de canais (SUB)
    // ------------------------------------------------------------------

    /** Inscreve-se em um canal, todas as inscrições usam a mesma conexão */
    private void subscribe(String channel) {
        if (subscriptions.add(channel)) {
            sub.subscribe(channel.getBytes(StandardCharsets.UTF_8));
            note("inscrito no canal '" + channel + "' (" + subscriptions.size() + " inscrição(ões))");
        }
    }

    private void pause(long ms) {
        long deadline = System.currentTimeMillis() + ms;
        long remaining;
        while ((remaining = deadline - System.currentTimeMillis()) > 0) {
            sub.setReceiveTimeOut((int) remaining);
            receiveOne();
        }
    }

    /** recebe e exibe) uma publicação, se chegar antes do timeout configurado no socket */
    private void receiveOne() {
        String topic = sub.recvStr();                      // frame 1: tópico (null = timeout)
        if (topic == null || !sub.hasReceiveMore()) {
            return;
        }
        byte[] payload = sub.recv(0);                      // frame 2: Envelope serializado
        handlePublication(topic, payload);
    }

    private void handlePublication(String topic, byte[] payload) {
        try {
            Envelope env = Envelope.parseFrom(payload);
            if (env.getBodyCase() != Envelope.BodyCase.CHANNEL_MSG) {
                return;
            }
            String channel = env.getChannelMsg().getChannel();
            if (!topic.equals(channel) || !subscriptions.contains(channel)) {
                return;
            }
            observeClock(env.getLogicalClock());
            // Exibe canal, mensagem, timestamp do envio (ts=), relógio lógico de quem enviou (lc=),
            // hora do recebimento (received=) e o relógio lógico local após a atualização (local_lc=).
            log("RECV", env, "  received=" + LocalTime.now().format(CLOCK) + "  local_lc=" + logicalClock);
        } catch (InvalidProtocolBufferException e) {
            note("publicação inválida ignorada no tópico '" + topic + "'");
        }
    }

    // ------------------------------------------------------------------
    // Comportamento do bot
    // ------------------------------------------------------------------

    /**  se há menos de minChannels canais, cria um com nome ainda livre */
    private void ensureChannels(List<String> existing) {
        if (existing.size() >= minChannels) {
            return;
        }
        List<String> candidates = new ArrayList<>(CHANNEL_POOL);
        candidates.removeIf(c -> existing.stream().anyMatch(c::equalsIgnoreCase));
        String name = candidates.isEmpty()
                ? "canal-" + Integer.toHexString(random.nextInt(0xFFFFF))
                : candidates.get(random.nextInt(candidates.size()));
        createChannel(name);   // se outro bot criou antes, o servidor responde "já existe"
    }

    /**se está inscrito em menos de minSubscriptions canais, inscreve-se em mais um aleatório */
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

        while (true) {
            List<String> channels = listChannels();
            if (channels.size() < minChannels) {
                ensureChannels(channels);
                channels = listChannels();   // atualiza a lista com o canal recém criado
            }
            ensureSubscriptions(channels);

            if (channels.isEmpty()) {
                pause(1000);
                continue;
            }

            // escolhe um canal e envia burstSize mensagens
            String target = channels.get(random.nextInt(channels.size()));
            for (int i = 0; i < burstSize; i++) {
                if (!publish(target, randomText())) {
                    note("publicação em '" + target + "' falhou");
                }
                pause(messageIntervalMs);   // espera 1 s ainda recebendo publicações
            }
        }
    }

    @Override
    public void close() {
        ctx.close();   // fecha todos os sockets criados por este contexto
    }

    // ------------------------------------------------------------------
    public static void main(String[] args) throws InterruptedException {
        try (Bot bot = new Bot()) {
            bot.run();
        }
    }
}
