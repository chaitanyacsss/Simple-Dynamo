package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.TreeMap;


public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = "content provider: ";
    private TreeMap<String, String> nodes_ring = new TreeMap<String, String>();
    static final String PORT0 = "11108";
    static final String PORT1 = "11112";
    static final String PORT2 = "11116";
    static final String PORT3 = "11120";
    static final String PORT4 = "11124";
    static String my_port = null;
    static final int SERVER_PORT = 10000;
    static final String AND_DELIMITER = "&";
    static final String AT_DELIMITER = "@";
    static final String STAR_DELIMITER = "*";
    String FIRST_SUCCESSOR_PORT = null;
    String SECOND_SUCCESSOR_PORT = null;
    String PREDECESSOR_PORT = null;
    String PRE_PREDECESSOR_PORT = null;
    static final String REQUESTING_YOUR_KEYS = "requestyourkeys";
    static final String REQUESTING_OWN_KEYS = "requestownkeys";
    static final String REQUESTING_ALL_KEYS = "requestingallkeys";
    static final String QUERY_KEY = "querykey";
    static final String DELETE_KEY = "deletekey";
    static final String INSERT_KEY_ON_REPLICA = "insertKeyOnReplica";
    static final String INSERT_KEY_FROM_REQUEST_PORT = "insertKeyFromRequestPort";
    static final String QUERY_KEY_SECOND_SUCCESSOR = "querykeyonsecondsuccessor";
    static final String QUERY_KEY_FIRST_SUCCESSOR = "querykeyonfirstsuccessor";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            if (selection.equals(STAR_DELIMITER)) {
                /**first delete local entries**/
                deleteAllLocally();

                /**send to all other nodes for delete **/
                for (String key : nodes_ring.keySet()) {
                    if (!nodes_ring.get(key).equals(my_port)) {
                        Log.d(TAG, "sending query to successor with * selection");
                        String delete_message_details = DELETE_KEY + AND_DELIMITER + selection + AND_DELIMITER + my_port + "\n";
                        try {
                            Socket node_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes_ring.get(key)));

                            node_socket.getOutputStream().write(delete_message_details.getBytes());
                            node_socket.getOutputStream().flush();

                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                Log.d(TAG, "while sleeping in delete: " + e.getMessage());
                            }
                            node_socket.close();
                        } catch (IOException e) {
                            Log.d(TAG, e.getMessage());
                        }
                    }
                }


            } else if (selection.equals((AT_DELIMITER))) {
                return deleteAllLocally();
            } else {
                if (getHomePortForMessage(selection).contains(my_port)) {
                    deleteSelectionLocally(selection);
                } else {
                    String delete_message_details = DELETE_KEY + AND_DELIMITER + selection + AND_DELIMITER + my_port + "\n";
                    for (String port : getHomePortForMessage(selection)) {
                        sendMessageToPort(delete_message_details, port);
                    }
                }

            }
        } catch (Exception e) {
            Log.e(TAG, "query: Exception", e);
        }


        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        try {
            Log.d(TAG, "query started locally for selection = " + selection);
            if (selection.equals(STAR_DELIMITER)) {
                Log.d(TAG, "STAR QUERY STARTED ON THIS PORT");
                /**first collect all local items**/
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                String local_all = queryAllLocallyForString();
                if (local_all != null) {
                    String[] local_all_list = local_all.split(AND_DELIMITER);
                    for (int i = 0; i < local_all_list.length; i = i + 2) {
                        matrixCursor.addRow(new Object[]{local_all_list[i], local_all_list[i + 1]});
                    }
                }

                for (String key : nodes_ring.keySet()) {
                    if (!nodes_ring.get(key).equals(my_port)) {
                        Log.d(TAG, "sending query to successor with * selection");
                        String query_message_details = QUERY_KEY + AND_DELIMITER + selection + AND_DELIMITER + my_port + "\n";
                        try {
                            Socket node_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodes_ring.get(key)));

                            node_socket.getOutputStream().write(query_message_details.getBytes());
                            node_socket.getOutputStream().flush();

                            BufferedReader in;

                            try {
                                Thread.sleep(25);
                            } catch (InterruptedException e) {
                                Log.d(TAG, "while sleeping: " + e.getMessage());
                            }

                            Boolean looper = Boolean.TRUE;
                            String readMsg = null;
                            while (looper) {
                                in = new BufferedReader(new InputStreamReader(node_socket.getInputStream()));
                                readMsg = in.readLine();
                                if (readMsg != null) {
                                    looper = false;
                                } else {
                                    in.close();
                                }
                            }
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                Log.d(TAG, "while sleeping: " + e.getMessage());
                            }
                            if (readMsg != null) {
                                Log.d(TAG, "received response for star query!");
                                String[] remote_single_node_all = readMsg.split(AND_DELIMITER);
                                for (int i = 0; i < remote_single_node_all.length; i = i + 2) {
                                    matrixCursor.addRow(new Object[]{remote_single_node_all[i], remote_single_node_all[i + 1]});
                                }
                            }
                            node_socket.close();
                        } catch (IOException e) {
                            Log.d(TAG, e.getMessage());
                        }
                    }
                }

                return matrixCursor;
            } else if (selection.equals((AT_DELIMITER))) {
                Log.d(TAG, "query: querying @");
                return queryAllLocally();
            } else if (getHomePortForMessage(selection).get(2).equals(my_port)) {
                Log.d(TAG, "querying locally because file hash falls in range");
                while (querySelectionLocally(selection) == null) {
                    Thread.sleep(20);
                }
                return querySelectionLocally(selection);
            } else {
                /**Send to the second replica location to check and query**/
                Log.d(TAG, "sending query to second successor");
                // initialize matrix cursor to add response key
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                try {
                    String query_message_details = QUERY_KEY_SECOND_SUCCESSOR + AND_DELIMITER + selection + AND_DELIMITER + my_port + "\n";
                    Socket second_successor_node_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(getHomePortForMessage(selection).get(2)));

                    second_successor_node_socket.getOutputStream().write(query_message_details.getBytes());
                    second_successor_node_socket.getOutputStream().flush();
                    Log.d(TAG, "selection sent to second successor: " + selection);

                    BufferedReader in;
                    Thread.sleep(25);
                    Boolean looper = Boolean.TRUE;
                    String readMsg = null;
                    while (looper) {
                        in = new BufferedReader(new InputStreamReader(second_successor_node_socket.getInputStream()));
                        readMsg = in.readLine();
                        if (readMsg != null) {
                            looper = false;
                        } else {
                            in.close();
                        }
                    }
                    Thread.sleep(20);

                    if (readMsg != null) {
                        Log.d(TAG, "received response for selection query!");
                        String[] remote_single_selection = readMsg.split(AND_DELIMITER);
                        for (int i = 0; i < remote_single_selection.length; i = i + 2) {
                            matrixCursor.addRow(new Object[]{remote_single_selection[i], remote_single_selection[i + 1]});
                        }
                    }

                    second_successor_node_socket.close();

                } catch (IOException e) {
                    Log.d(TAG, "error querying second replica: " + e.getMessage());
                    /** Error implies the second successor is dead. so query the first successor**/
                    Socket first_successor_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(getHomePortForMessage(selection).get(1)));
                    String query_message_details_new = QUERY_KEY_FIRST_SUCCESSOR + AND_DELIMITER + selection + AND_DELIMITER + my_port + "\n";
                    first_successor_socket.getOutputStream().write(query_message_details_new.getBytes());
                    first_successor_socket.getOutputStream().flush();

                    BufferedReader in;
                    Thread.sleep(25);

                    Boolean looper = Boolean.TRUE;
                    String readMsg = null;
                    while (looper) {
                        in = new BufferedReader(new InputStreamReader(first_successor_socket.getInputStream()));
                        readMsg = in.readLine();
                        if (readMsg != null) {
                            looper = false;
                        } else {
                            in.close();
                        }
                    }
                    Thread.sleep(20);
                    if (readMsg != null) {
                        Log.d(TAG, "received response for star query!");
                        String[] remote_single_selection = readMsg.split(AND_DELIMITER);
                        for (int i = 0; i < remote_single_selection.length; i = i + 2) {
                            matrixCursor.addRow(new Object[]{remote_single_selection[i], remote_single_selection[i + 1]});
                        }
                    }

                    first_successor_socket.close();
                } finally {
                    Log.d(TAG, "returning matrix cursor");
                    return matrixCursor;
                }

            }
        } catch (Exception e) {
            Log.e(TAG, "query: Exception", e);
        }


        return null;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.get("key").toString();

        String msg = values.get("value").toString();
        Log.d(TAG, "insert called locally: " + key);
        try {

            /**check for home port of the message**/
            List<String> threeports = getHomePortForMessage(key);
            Log.d(TAG, "insert: " + key + " The three ports for this key are : " + threeports.get(0) + " " + threeports.get(1) + " " + threeports.get(2));
            if (threeports.get(0).equals(my_port)) {
                writeToFile(values, key, msg);
                /**send to replicas**/
                Log.d(TAG, "This is the home port of the key : " + key);
                String replica_message_details = INSERT_KEY_ON_REPLICA + AND_DELIMITER + key + AND_DELIMITER + msg;
                Log.d(TAG, "sending key to replicas : " + key);
                sendMessageToPort(replica_message_details, FIRST_SUCCESSOR_PORT);
                sendMessageToPort(replica_message_details, SECOND_SUCCESSOR_PORT);

            } else {
                String insert_message_details = INSERT_KEY_FROM_REQUEST_PORT + AND_DELIMITER + key + AND_DELIMITER + msg;

                List<String> home_ports_for_message = getHomePortForMessage(key);
                sendMessageToPort(insert_message_details, home_ports_for_message.get(0));
                sendMessageToPort(insert_message_details, home_ports_for_message.get(1));
                sendMessageToPort(insert_message_details, home_ports_for_message.get(2));

                // TODO : may be add a timeout or something?
            }
        } catch (Exception e) {
            Log.e(TAG, "insert: Exception: ", e);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

        /**getting my port and port-string**/
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            my_port = myPort;
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        Log.d(TAG, "onCreate: Im alive!");
        /**Adding all nodes to the ring**/
        try {
            Log.d(TAG, "onCreate: adding nodes to ring");
            nodes_ring.put(getHashFromPort(PORT0), PORT0);
            nodes_ring.put(getHashFromPort(PORT1), PORT1);
            nodes_ring.put(getHashFromPort(PORT2), PORT2);
            nodes_ring.put(getHashFromPort(PORT3), PORT3);
            nodes_ring.put(getHashFromPort(PORT4), PORT4);

            List<String> neighbours = getNeighbours(my_port);
            FIRST_SUCCESSOR_PORT = neighbours.get(0);
            SECOND_SUCCESSOR_PORT = neighbours.get(1);
            PREDECESSOR_PORT = neighbours.get(2);
            PRE_PREDECESSOR_PORT = neighbours.get(3);

            // clear local files before
            deleteAllLocally();

            // request my keys from other ports
            String requesting_keys = REQUESTING_ALL_KEYS;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requesting_keys, my_port);
            Thread.sleep(100);
        } catch (Exception e) {
            Log.d(TAG, "onCreate: ", e);
        }

        return false;
    }

    private String getHashFromPort(String port) {
        try {
            //Log.d(TAG, "getHashFromPort: port is: " + port);
            return genHash(String.valueOf((Integer.parseInt(port) / 2)));
        } catch (NoSuchAlgorithmException e) {
            Log.d(TAG, "getHashFromPort", e);
            return null;
        }
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void writeToFile(ContentValues values, String key, String msg) {
        FileOutputStream outputStream;
        Log.d(TAG, "writeToFile: " + key + " " + msg);
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(msg.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("GroupMessengerProvider", "File write failed");
        }
        Log.v("insert", key);
    }

    private List<String> getNeighbours(String port) {
        List<String> neighbours = new ArrayList<String>();
        String first_successor_port;
        String second_successor_port;
        String predecessor_port;
        String pre_predecessor_port;
        if (nodes_ring.lastEntry().getValue().equals(port)) {
            first_successor_port = nodes_ring.firstEntry().getValue();
            second_successor_port = nodes_ring.higherEntry(getHashFromPort(first_successor_port)).getValue();

            // when my port is last but 1
        } else if (nodes_ring.higherEntry(getHashFromPort(port)).getValue().equals(nodes_ring.lastEntry().getValue())) {
            first_successor_port = nodes_ring.higherEntry(getHashFromPort(port)).getValue();
            second_successor_port = nodes_ring.firstEntry().getValue();
        } else {
            first_successor_port = nodes_ring.higherEntry(getHashFromPort(port)).getValue();
            second_successor_port = nodes_ring.higherEntry(getHashFromPort(first_successor_port)).getValue();
        }

        // when my port is first entry
        if (nodes_ring.firstEntry().getValue().equals(port)) {
            predecessor_port = nodes_ring.lastEntry().getValue();
            pre_predecessor_port = nodes_ring.lowerEntry(getHashFromPort(predecessor_port)).getValue();
            // when my port is second
        } else if (nodes_ring.higherEntry(nodes_ring.firstKey()).getValue().equals(port)) {
            predecessor_port = nodes_ring.firstEntry().getValue();
            pre_predecessor_port = nodes_ring.lastEntry().getValue();
        } else {
            predecessor_port = nodes_ring.lowerEntry(getHashFromPort(port)).getValue();
            pre_predecessor_port = nodes_ring.lowerEntry(getHashFromPort(predecessor_port)).getValue();
        }

        neighbours.add(first_successor_port);
        neighbours.add(second_successor_port);
        neighbours.add(predecessor_port);
        neighbours.add(pre_predecessor_port);
        return neighbours;

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            mainLoop:
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    Log.d(TAG, "servertask: accepted connection");
                    InputStream stream = clientSocket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(stream));
                    Log.d(TAG, "servertask: opened reader");
                    String readMsg = in.readLine();
                    if (readMsg == null) {
                        Log.d(TAG, "server task: inline read null!");
                        continue mainLoop;
                    }
                    System.out.println("read msg at server  " + readMsg);
                    List<String> message_items = Arrays.asList(readMsg.split(AND_DELIMITER));
                    String message_type = message_items.get(0);

                    if (message_type.equals(INSERT_KEY_FROM_REQUEST_PORT) || message_type.equals(INSERT_KEY_ON_REPLICA)) {
                        Log.d(TAG, "server task: insert key received from request port");
                        String key = message_items.get(1);
                        String value = message_items.get(2);

                        ContentValues cv = new ContentValues();
                        cv.put("key", key);
                        cv.put("value", value);
                        writeToFile(cv, key, value);

                    } else if (message_type.equals(QUERY_KEY_SECOND_SUCCESSOR) || message_type.equals(QUERY_KEY_FIRST_SUCCESSOR)) {
                        Log.d(TAG, "server task message type: " + message_type);
                        String returnString = null;
                        while (returnString == null) {
                            returnString = querySelectionLocallyForString(message_items.get(1));
                        }
                        Log.d(TAG, "query on successor response ready to send back: " + returnString + " " + message_items.get(2));

                        clientSocket.getOutputStream().write(returnString.getBytes());
                        clientSocket.getOutputStream().flush();
                        Thread.sleep(20);
                        clientSocket.close();
                    } else if (message_type.equals(QUERY_KEY)) {
                        Log.d(TAG, "server task: query key");
                        String key = message_items.get(1);
                        String home_port = message_items.get(2);

                        String returnString = null;
                        if (key.equals(STAR_DELIMITER)) {
                            if (home_port.equals(my_port)) {
                            } else {
                                Log.d(TAG, "query: querying * on this node when request is sent from " + home_port);
                                returnString = queryAllLocallyForString();
                                clientSocket.getOutputStream().write(returnString.getBytes());
                                clientSocket.getOutputStream().flush();

                                try {
                                    Thread.sleep(20);
                                } catch (InterruptedException ex) {
                                    Log.d(TAG, "client task while sleeping: " + ex.getMessage());
                                }
                                clientSocket.close();
                            }
                        } else {
                            query(buildUri("content"), null, key, null, null);
                        }
                    } else if (message_type.equals(DELETE_KEY)) {
                        /**implement delete key**/
                        String key = message_items.get(1);
                        String home_port = message_items.get(2);

                        if (key.equals(STAR_DELIMITER)) {
                            if (!home_port.equals(my_port)) {
                                deleteAllLocally();
                            } else {
                                //do nothing
                            }
                        } else {
                            deleteSelectionLocally(key);
                        }

                    } else if (message_type.equals(REQUESTING_YOUR_KEYS)) {
                        String returnRequest = queryMineLocallyForString();
                        clientSocket.getOutputStream().write(returnRequest.getBytes());
                        clientSocket.getOutputStream().flush();

                        Log.d(TAG, "Port requested MY keys to be sent. " + message_items.get(1));
                        Thread.sleep(20);
                        clientSocket.close();
                    } else if (message_type.equals(REQUESTING_OWN_KEYS)) {
                        String home_port = message_items.get(1);
                        String returnRequest = queryRangeLocallyForString(home_port);
                        clientSocket.getOutputStream().write(returnRequest.getBytes());
                        clientSocket.getOutputStream().flush();

                        Log.d(TAG, "Port requested its keys to be sent. " + message_items.get(1));
                        Thread.sleep(20);
                        clientSocket.close();
                    }

                } catch (IOException e) {
                    Log.e(TAG, "server task IOException: " + e.getMessage());
                } catch (Exception e) {
                    Log.e(TAG, "server task Exception: " + e.getMessage());
                }

            }


        }


        public Integer keyCount = 0;

        protected void onProgressUpdate(String... strings) {

            ContentValues cv = new ContentValues();
            cv.put("key", Integer.toString(keyCount));
            cv.put("value", keyCount);
            keyCount++;
            insert(buildUri("content"), cv);

            return;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            System.out.println("my port: " + msgs[1]);

            String message = msgs[0];

            Log.d(TAG, "Client task : " + message);
            List<String> message_received_at_client_task = Arrays.asList(message.split(AND_DELIMITER));
            String message_type = message_received_at_client_task.get(0);
            if (message_type.equals(REQUESTING_ALL_KEYS)) {
                Log.d(TAG, "sending requests for all related keys!");
                try {
                    // first, sending to prepredecessor
                    String messageToSend = REQUESTING_YOUR_KEYS + AND_DELIMITER + my_port + "\n";
                    sendAndReceiveFromNode(messageToSend, PRE_PREDECESSOR_PORT);

                    // first, sending to predecessor
                    String messageToSendToPredecessor = REQUESTING_YOUR_KEYS + AND_DELIMITER + my_port + "\n";
                    sendAndReceiveFromNode(messageToSendToPredecessor, PREDECESSOR_PORT);

                    // request own keys from successor
                    String messageToSendToSuccessor = REQUESTING_OWN_KEYS + AND_DELIMITER + my_port + "\n";
                    sendAndReceiveFromNode(messageToSendToSuccessor, FIRST_SUCCESSOR_PORT);

                    // request own keys from second successor
                    String messageToSendToSecondSuccessor = REQUESTING_OWN_KEYS + AND_DELIMITER + my_port + "\n";
                    sendAndReceiveFromNode(messageToSendToSecondSuccessor, SECOND_SUCCESSOR_PORT);


                } catch (Exception e) {
                    Log.e(TAG, "ClientTask Exception: " + e.getMessage());
                }
            }
            return null;
        }
    }

    private void sendAndReceiveFromNode(String messageToSend, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));

            Log.d(TAG, "request ready! " + messageToSend);
            socket.getOutputStream().write(messageToSend.getBytes());

            Thread.sleep(25);

            socket.getOutputStream().flush();
            Log.d(TAG, "request sent! " + messageToSend);

            BufferedReader in;

            Thread.sleep(25);

            Boolean looper = Boolean.TRUE;
            String readMsg = null;
            while (looper) {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                readMsg = in.readLine();
                if (readMsg != null) {
                    looper = false;
                } else {
                    in.close();
                }
            }

            Log.d(TAG, "sendAndReceiveFromNode: got response from node! " + readMsg);

            Thread.sleep(25);

            if (readMsg != null) {
                Log.d(TAG, "received response for requesting keys!");
                String[] remote_single_node_all = readMsg.split(AND_DELIMITER);
                for (int i = 0; i < remote_single_node_all.length; i = i + 2) {
                    writeToFile(null, remote_single_node_all[i], remote_single_node_all[i + 1]);
                }
            }

            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "sendAndReceiveFromNode socket Exception: " + e.getMessage());
        } catch (Exception e) {
            Log.e(TAG, "sendAndReceiveFromNode Exception: " + e.getMessage());
        }
    }

    private List<String> getHomePortForMessage(String messageKey) {
        List<String> threePorts = new ArrayList<String>();
        String homePort = null;
        String fileKeyHash = null;
        try {
            fileKeyHash = genHash(messageKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (fileKeyHash.compareTo(nodes_ring.firstKey()) <= 0 || fileKeyHash.compareTo(nodes_ring.lastKey()) > 0) {
            Log.d(TAG, "getHomePortForMessage: first port is home port for this key " + messageKey);
            homePort = nodes_ring.firstEntry().getValue();
        }
        for (String key : nodes_ring.keySet()) {
            if (!key.equals(nodes_ring.firstKey())) {
                if (fileKeyHash.compareTo(key) <= 0 && fileKeyHash.compareTo(nodes_ring.lowerKey(key)) > 0) {
                    homePort = nodes_ring.get(key);
                    break;
                }
            }
        }
        Log.d(TAG, "getHomePortForMessage: home port for key is " + messageKey + " " + homePort);
        threePorts.add(homePort);
        threePorts.add(getNeighbours(homePort).get(0));
        threePorts.add(getNeighbours(homePort).get(1));
        return threePorts;
    }

    private void sendMessageToPort(String message, String port) {
        try {
            Log.d(TAG, "sendMessageToPort: " + message + " " + port);
            message = message + "\n";
            Socket successor_node_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));

            successor_node_socket.getOutputStream().write(message.getBytes());
            successor_node_socket.getOutputStream().flush();

            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Log.d(TAG, "client task while sleeping: " + e.getMessage());
            }

            successor_node_socket.close();

        } catch (IOException e) {
            Log.e(TAG, "failed when sending message to port: ", e);
        }
    }

    private int deleteSelectionLocally(String selection) {

        try {
            getContext().deleteFile(selection);
        } catch (Exception e) {
            Log.e(TAG, "deleteSelectionLocally: ", e);
            return 1;
        }
        return 0;
    }

    private int deleteAllLocally() {
        File[] files = new File(getContext().getFilesDir().getAbsolutePath()).listFiles();
        for (File file : files) {

            try {
                getContext().deleteFile(file.getName());
            } catch (Exception e) {
                Log.e(TAG, "deleteAllLocally: ", e);
                return 1;
            }


        }
        return 0;
    }

    private Cursor querySelectionLocally(String selection) {
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        String value = null;
        try {
            FileInputStream inputStream = getContext().openFileInput(selection);
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

            value = in.readLine();
        } catch (Exception e) {
            Log.d(TAG, "querySelectionLocally: no file found");
            return null;
        }
        matrixCursor.addRow(new Object[]{selection, value});
        return matrixCursor;
    }

    private String querySelectionLocallyForString(String selection) {
        String value;
        String keyValue;
        try {
            FileInputStream inputStream = getContext().openFileInput(selection);
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

            value = in.readLine();
            keyValue = selection + AND_DELIMITER + value;
        } catch (Exception e) {
            Log.e(TAG, "querySelectionLocallyForString: " + e.getMessage());
            return null;
        }
        return keyValue;
    }

    private Uri buildUri(String scheme) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private String queryAllLocallyForString() {
        File[] files = new File(getContext().getFilesDir().getAbsolutePath()).listFiles();
        String linkedString = null;
        for (File file : files) {

            String value = null;
            try {
                FileInputStream inputStream = getContext().openFileInput(file.getName());
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                value = in.readLine();

                if (linkedString == null) {
                    linkedString = file.getName() + AND_DELIMITER + value;
                } else {
                    linkedString = linkedString + AND_DELIMITER + file.getName() + AND_DELIMITER + value;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Log.d(TAG, "queryAllLocally: " + file.getName() + "  value : " + value);

        }
        return linkedString;
    }

    private String queryMineLocallyForString() {
        File[] files = new File(getContext().getFilesDir().getAbsolutePath()).listFiles();
        String linkedString = null;
        for (File file : files) {

            String value = null;
            try {
                FileInputStream inputStream = getContext().openFileInput(file.getName());
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                value = in.readLine();
                if (getHomePortForMessage(file.getName()).get(0).equals(my_port)) {
                    if (linkedString == null) {
                        linkedString = file.getName() + AND_DELIMITER + value;
                    } else {
                        linkedString = linkedString + AND_DELIMITER + file.getName() + AND_DELIMITER + value;
                    }
                    Log.d(TAG, "queryMineLocallyForString: " + file.getName() + "  value : " + value);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return linkedString;
    }

    private String queryRangeLocallyForString(String port) {
        File[] files = new File(getContext().getFilesDir().getAbsolutePath()).listFiles();
        String linkedString = null;
        for (File file : files) {

            String value = null;
            try {
                FileInputStream inputStream = getContext().openFileInput(file.getName());
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                value = in.readLine();
                if (getHomePortForMessage(file.getName()).get(0).equals(port)) {
                    if (linkedString == null) {
                        linkedString = file.getName() + AND_DELIMITER + value;
                    } else {
                        linkedString = linkedString + AND_DELIMITER + file.getName() + AND_DELIMITER + value;
                    }
                    Log.d(TAG, "queryRangeLocallyForString: " + file.getName() + "  value : " + value);
                }
            } catch (Exception e) {
                Log.e(TAG, "queryRangeLocallyForString: ", e);
            }
        }
        return linkedString;
    }

    private Cursor queryAllLocally() {
        File[] files = new File(getContext().getFilesDir().getAbsolutePath()).listFiles();
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        for (File file : files) {

            String value = null;
            try {
                FileInputStream inputStream = getContext().openFileInput(file.getName());
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                value = in.readLine();
            } catch (Exception e) {
                e.printStackTrace();
            }
            Log.d(TAG, "queryAllLocally: " + file.getName() + "  value : " + value);
            matrixCursor.addRow(new Object[]{file.getName(), value});

        }
        return matrixCursor;
    }

}
