/*
 * Classname:   BPortForwording
 *
 * Version:     1.0
 *
 * Date:        4/6/2022
 *
 * Copyright (c) 2022 PHOENIX CONTACT
 */

package com.ste.portForwarding;

import com.tridium.ndriver.datatypes.BIpAddress;

import javax.baja.nre.annotations.Facet;
import javax.baja.nre.annotations.NiagaraProperty;
import javax.baja.nre.annotations.NiagaraType;
import javax.baja.sys.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * TODO Description
 *
 * @author songyantao@phoenixcontact.com.cn
 * @version 1.0
 * @since 4/6/2022
 */

@NiagaraType
@NiagaraProperty(name = "enable", type = "boolean", defaultValue = "true")
@NiagaraProperty(name = "localPort", type = "int", defaultValue = "9000", facets = {@Facet(name = "BFacets.MIN", value = "1"), @Facet(name = "BFacets.MAX", value = "65535")})
@NiagaraProperty(name = "connectionLimit", type = "int", defaultValue = "5", facets = {@Facet(name = "BFacets.MIN", value = "1"), @Facet(name = "BFacets.MAX", value = "10")})
@NiagaraProperty(name = "target", type = "BIpAddress", defaultValue = "new BIpAddress(\"192.168.2.132\",502)")
@NiagaraProperty(name = "valid", type = "boolean", defaultValue = "false", flags = Flags.TRANSIENT | Flags.READONLY, facets = {@Facet(name = "BFacets.TRUE_TEXT", value = "\"VALID\""), @Facet(name = "BFacets.FALSE_TEXT", value = "\"INVALID\"")})
public class BPortForwarding extends BComponent {





    /*+ ------------ BEGIN BAJA AUTO GENERATED CODE ------------ +*/
    /*@ $com.ste.portForwarding.BPortForwording(1127523696)1.0$ @*/
    /* Generated Wed Apr 06 15:20:56 CST 2022 by Slot-o-Matic (c) Tridium, Inc. 2012 */

////////////////////////////////////////////////////////////////
// Property "enable"
////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code enable} property.
     * @see #getEnable
     * @see #setEnable
     */
    public static final Property enable = newProperty(0, true, null);

    /**
     * Get the {@code enable} property.
     * @see #enable
     */
    public boolean getEnable() { return getBoolean(enable); }

    /**
     * Set the {@code enable} property.
     * @see #enable
     */
    public void setEnable(boolean v) { setBoolean(enable, v, null); }

////////////////////////////////////////////////////////////////
// Property "localPort"
////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code localPort} property.
     * @see #getLocalPort
     * @see #setLocalPort
     */
    public static final Property localPort = newProperty(0, 9000, BFacets.make(BFacets.make(BFacets.MIN, 1), BFacets.make(BFacets.MAX, 65535)));

    /**
     * Get the {@code localPort} property.
     * @see #localPort
     */
    public int getLocalPort() { return getInt(localPort); }

    /**
     * Set the {@code localPort} property.
     * @see #localPort
     */
    public void setLocalPort(int v) { setInt(localPort, v, null); }

////////////////////////////////////////////////////////////////
// Property "connectionLimit"
////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code connectionLimit} property.
     * @see #getConnectionLimit
     * @see #setConnectionLimit
     */
    public static final Property connectionLimit = newProperty(0, 5, BFacets.make(BFacets.make(BFacets.MIN, 1), BFacets.make(BFacets.MAX, 10)));

    /**
     * Get the {@code connectionLimit} property.
     * @see #connectionLimit
     */
    public int getConnectionLimit() { return getInt(connectionLimit); }

    /**
     * Set the {@code connectionLimit} property.
     * @see #connectionLimit
     */
    public void setConnectionLimit(int v) { setInt(connectionLimit, v, null); }

////////////////////////////////////////////////////////////////
// Property "target"
////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code target} property.
     * @see #getTarget
     * @see #setTarget
     */
    public static final Property target = newProperty(0, new BIpAddress("192.168.2.132",502), null);

    /**
     * Get the {@code target} property.
     * @see #target
     */
    public BIpAddress getTarget() { return (BIpAddress)get(target); }

    /**
     * Set the {@code target} property.
     * @see #target
     */
    public void setTarget(BIpAddress v) { set(target, v, null); }

////////////////////////////////////////////////////////////////
// Property "valid"
////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code valid} property.
     * @see #getValid
     * @see #setValid
     */
    public static final Property valid = newProperty(Flags.TRANSIENT | Flags.READONLY, false, BFacets.make(BFacets.make(BFacets.TRUE_TEXT, "VALID"), BFacets.make(BFacets.FALSE_TEXT, "INVALID")));

    /**
     * Get the {@code valid} property.
     * @see #valid
     */
    public boolean getValid() { return getBoolean(valid); }

    /**
     * Set the {@code valid} property.
     * @see #valid
     */
    public void setValid(boolean v) { setBoolean(valid, v, null); }

////////////////////////////////////////////////////////////////
// Type
////////////////////////////////////////////////////////////////

    @Override
    public Type getType() { return TYPE; }
    public static final Type TYPE = Sys.loadType(BPortForwarding.class);

    /*+ ------------ END BAJA AUTO GENERATED CODE -------------- +*/
    ExecutorService poolExecutor = Executors.newCachedThreadPool();
    ServerSocket serverSocket;
    List<Connection> connections = new ArrayList<>();

    Future<?> serverFuture;

    @Override
    public void started() throws Exception {
        super.started();
        this.init();
    }

    public void stopped(){
        closeAll();
    }

    private boolean tryConnectClient() {
        try {
            Socket remoteServerSocket = new Socket(this.getTarget().getIpAddress(), this.getTarget().getPort());
            remoteServerSocket.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void init() {
        if (!this.getEnable()) {
            return;
        }
        serverFuture = poolExecutor.submit(() -> {
            while (this.getEnable()) {
                this.closeAll();
                try {
                    serverSocket = new ServerSocket(this.getLocalPort());
                } catch (Exception e) {
                    serverSocket = null;
                    this.setValid(false);
                    try {
                        Thread.sleep(5000);
                    } catch (Exception ignore) {

                    }
                    continue;
                }
                this.setValid(tryConnectClient());
                while (getEnable()) {
                    try {
                        Socket clientSocket = null;
                        Socket remoteServerSocket = null;
                        clientSocket = serverSocket.accept();
                        try {
                            remoteServerSocket = new Socket(this.getTarget().getIpAddress(), this.getTarget().getPort());
                        } catch (ConnectException ce) {
                            try {
                                clientSocket.close();
                            } catch (Exception ignore) {
                            }
                            continue;
                        }
                        this.setValid(true);
                        if (this.connections.size() >= this.getConnectionLimit()) {
                            try {
                                clientSocket.close();
                            } catch (Exception ignore) {
                            }
                            try {
                                remoteServerSocket.close();
                            } catch (Exception ignore) {
                            }
                        } else {
                            connections.add(new Connection(clientSocket, remoteServerSocket));
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                        closeAll();
                        try {
                            Thread.sleep(5000);
                        } catch (Exception ignore) {

                        }
                        break;
                    }
                }


            }

        });

    }

    void closeAll() {
        this.setValid(false);
        this.connections.forEach(Connection::cancel);
        this.connections.clear();
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (Exception e) {
            }
        }

        if (serverFuture != null) {
            serverFuture.cancel(true);
            serverFuture = null;
        }
    }

    class Connection {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Connection that = (Connection) o;
            return Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid);
        }

        UUID uuid;
        Future<?> f1;
        Future<?> f2;
        Socket sock1;
        Socket sock2;
        boolean canceled;


        public Connection(Socket sock1, Socket sock2) {
            this.uuid = UUID.randomUUID();
            this.sock1 = sock1;
            this.sock2 = sock2;
            f1 = poolExecutor.submit(() -> {
                try {
                    InputStream in = sock1.getInputStream();
                    OutputStream out = sock2.getOutputStream();
                    long lastTime = System.currentTimeMillis();
                    while (true) {
                        //读入数据
                        byte[] data = new byte[1024];
                        int readlen = in.read(data);
                        if (System.currentTimeMillis() - lastTime > 60000L) {
                            break;
                        }
                        if (sock1.isClosed() || sock2.isClosed()) {
                            break;
                        }
                        //如果没有数据，则暂停
                        if (readlen <= 0) {
                            Thread.sleep(300);
                            continue;
                        }
                        lastTime = System.currentTimeMillis();
                        out.write(data, 0, readlen);
                        out.flush();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    BPortForwarding.this.connections.remove(Connection.this);
                    Connection.this.cancel();
                }
            });
            f2 = poolExecutor.submit(() -> {
                try {
                    InputStream in = sock2.getInputStream();
                    OutputStream out = sock1.getOutputStream();
                    long lastTime = System.currentTimeMillis();
                    while (true) {
                        //读入数据
                        byte[] data = new byte[1024];
                        int readlen = in.read(data);
                        if (System.currentTimeMillis() - lastTime > 60000L) {
                            break;
                        }
                        if (sock1.isClosed() || sock2.isClosed()) {
                            break;
                        }
                        //如果没有数据，则暂停
                        if (readlen <= 0) {
                            Thread.sleep(300);
                            continue;
                        }
                        lastTime = System.currentTimeMillis();
                        out.write(data, 0, readlen);
                        out.flush();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    BPortForwarding.this.connections.remove(Connection.this);
                    Connection.this.cancel();
                }
            });
        }

        void cancel() {
            if (canceled) {
                return;
            }
            canceled = true;
            try {
                if (sock1 != null) {
                    sock1.close();
                }
            } catch (Exception exx) {
            }
            try {
                if (sock2 != null) {
                    sock2.close();
                }
            } catch (Exception exx) {

            }
            f1.cancel(true);
            f2.cancel(true);
        }
    }


    @Override
    public void changed(Property property, Context context) {
        if (!this.isRunning()) {
            return;
        }
        super.changed(property, context);

        if (property == enable || property == localPort) {
            if (this.getEnable()) {
                this.init();
            } else {
                this.closeAll();
            }
        }

    }


    private final Logger log = Logger.getLogger(getClass().getSimpleName());

    public BPortForwarding() {
    }
}
