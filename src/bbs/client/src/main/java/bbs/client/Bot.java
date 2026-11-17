package bbs.client;

import bbs.proto.CreateChannelRequest;
import bbs.proto.Envelope;
import bbs.proto.ListChannelsRequest;
import bbs.proto.LoginRequest;
import bbs.proto.Status;
import com.google.protobuf.InvalidProtocolBufferException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *   1. Faz login (repete com outro nome se o servidor devolver erro).
 *   2. Em ciclo: lista os canais e, se houver menos de MIN_CHANNELS,
 *      tenta criar um canal novo escolhido de uma lista de nomes.
 *
 * configuração por variáveis de ambiente (definidas no docker-compose):
 *   BROKER_ADDR      endereço do broker      (padrão tcp://broker:5555)
 *   BOT_NAME         nome base do bot        (padrão bot-xxxx aleatório)
 *   MIN_CHANNELS     critério para criar     (padrão 3)
 *   INTERVAL_SECONDS pausa entre os ciclos   (padrão 10)
 */
public class Bot implements AutoCloseable {

    private static final int TIMEOUT_MS = 5000;   // espera máxima por uma resposta
    private static final int MAX_RETRIES = 5;     // tentativas por requisição
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    // nomes para canais novos
    private static final List<String> CHANNEL_POOL =
            List.of("geral", "aleatório", "tecnologia", "jogos", "musica", "filmes");

    private final String brokerAddr;
    private final int minChannels;
    private final long intervalMs;
    private final Random random = new Random();

    private final ZContext ctx = new ZContext();
    private ZMQ.Socket socket;
    private String username;

    public Bot(String brokerAddr, String username, int minChannels, long intervalMs) {
        this.brokerAddr = brokerAddr;
        this.username = username;
        this.minChannels = minChannels;
        this.intervalMs = intervalMs;
        connect();
    }

    // ------------------------------------------------------------------
    // Rede
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
            System.out.printf("[%s] [%s] sem resposta (tentativa %d/%d), reconectando...%n",
                    LocalTime.now().format(CLOCK), username, attempt, MAX_RETRIES);
            connect();
        }
        throw new IllegalStateException("servidor indisponível");
    }

    /** timestamp do envio */
    private static Envelope.Builder envelope() {
        return Envelope.newBuilder().setTimestamp(System.currentTimeMillis());
    }

    /** print da mensagem */
    private void log(String direction, Envelope env) {
        String kind = env.getBodyCase().name().toLowerCase();
        String content = env.toString().trim().replaceAll("\\s*\\n\\s*", " ");
        System.out.printf("[%s] [%s] %s %-20s | %s%n",
                LocalTime.now().format(CLOCK), username, direction, kind, content);
    }

    // ------------------------------------------------------------------
    // Operações do protocolo
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
            // caso de erro ele tenta de novo com um nome aleatório
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

    // ------------------------------------------------------------------
    // comportamento do bot
    // ------------------------------------------------------------------

    private void maybeCreateChannel(List<String> existing) {
        if (existing.size() >= minChannels) {
            return;
        }
        List<String> candidates = new ArrayList<>(CHANNEL_POOL);
        candidates.removeIf(c -> existing.stream().anyMatch(c::equalsIgnoreCase));
        if (candidates.isEmpty()) {
            return;
        }
        createChannel(candidates.get(random.nextInt(candidates.size())));
    }

    public void run() throws InterruptedException {
        login();
        while (true) {
            maybeCreateChannel(listChannels());
            Thread.sleep(intervalMs);
        }
    }

    @Override
    public void close() {
        ctx.close();
    }

    // ------------------------------------------------------------------
    public static void main(String[] args) throws InterruptedException {
        String addr = System.getenv().getOrDefault("BROKER_ADDR", "tcp://broker:5555");
        String name = System.getenv().getOrDefault("BOT_NAME",
                "bot-" + Integer.toHexString(new Random().nextInt(0xFFFF)));
        int min = Integer.parseInt(System.getenv().getOrDefault("MIN_CHANNELS", "3"));
        long interval = Long.parseLong(System.getenv().getOrDefault("INTERVAL_SECONDS", "10")) * 1000;

        try (Bot bot = new Bot(addr, name, min, interval)) {
            bot.run();
        }
    }
}
