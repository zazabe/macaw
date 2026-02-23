mod common;

use macaw_core::prelude::*;
use macaw_ws::prelude::*;

use common::{WsTestClient, start_test_server};
mod helpers;
use helpers::*;

#[tokio::test]
async fn test_recorder_multiple_ws_conns() -> Result<(), anyhow::Error> {
    let temp_file = tempfile::NamedTempFile::new()?;
    let test_file = temp_file.path().to_path_buf();
    let (server_addr, mut server_handle) = start_test_server().await?;
    let server_url = format!("ws://{}", server_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy_addr = macaw
        .add_ws_proxy(
            "ws_proxy1",
            "127.0.0.1:0",
            &server_url,
            WsProxyOptions::default(),
        )
        .await?;

    let proxy_url = format!("ws://{}/", proxy_addr);
    let mut client1 = WsTestClient::connect(&proxy_url).await?;
    let conn0 = server_handle.recv_connect().await?;

    let mut client2 = WsTestClient::connect(&proxy_url).await?;
    let conn1 = server_handle.recv_connect().await?;

    server_handle.send_to(conn0, "msg1 for conn0")?;
    server_handle.send_to(conn1, "msg1 for conn1")?;

    assert_eq!(client1.recv().await?, "msg1 for conn0");
    assert_eq!(client2.recv().await?, "msg1 for conn1");

    client1.send("hello from client1").await?;
    assert_eq!(
        server_handle.recv_message().await?,
        (conn0, "hello from client1")
    );
    server_handle.send_to(conn0, "echo: hello from client1")?;
    assert_eq!(client1.recv().await?, "echo: hello from client1");

    server_handle.send_to(conn0, "msg2 for conn0")?;
    server_handle.send_to(conn1, "msg2 for conn1")?;

    assert_eq!(client1.recv().await?, "msg2 for conn0");
    assert_eq!(client2.recv().await?, "msg2 for conn1");

    client2.send("hello from client2").await?;
    assert_eq!(
        server_handle.recv_message().await?,
        (conn1, "hello from client2")
    );
    server_handle.send_to(conn1, "echo: hello from client2")?;
    assert_eq!(client2.recv().await?, "echo: hello from client2");

    client1.close().await?;
    server_handle.assert_disconnected(conn0).await?;
    client2.close().await?;
    server_handle.assert_disconnected(conn1).await?;

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await?;

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file)?.as_str())?;
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.origin"# => "[origin]",
        r#".**.headers["sec-websocket-key"]"# => "[sec-websocket-key]",
        r#".**.headers["sec-websocket-version"]"# => "[sec-websocket-version]",
        r#".**.headers.connection"# => "[connection]",
        r#".**.headers.upgrade"# => "[upgrade]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg1 for conn0"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg1 for conn1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from client1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from client1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg2 for conn1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg2 for conn0"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from client2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from client2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": "Disconnect"
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": "Disconnect"
          }
        }
      ]
    }
    "#);
    Ok(())
}

#[tokio::test]
async fn test_recorder_multiple_ws_proxies() -> Result<(), anyhow::Error> {
    let temp_file = tempfile::NamedTempFile::new()?;
    let test_file = temp_file.path().to_path_buf();
    let (server_addr, mut server_handle) = start_test_server().await?;
    let server_url = format!("ws://{}", server_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy1_addr = macaw
        .add_ws_proxy(
            "ws_proxy1",
            "127.0.0.1:0",
            &server_url,
            WsProxyOptions::default(),
        )
        .await?;
    let proxy2_addr = macaw
        .add_ws_proxy(
            "ws_proxy2",
            "127.0.0.1:0",
            &server_url,
            WsProxyOptions::default(),
        )
        .await?;

    // Connect to proxy 1 and send a message
    let proxy1_url = format!("ws://{}/", proxy1_addr);
    let mut client1 = WsTestClient::connect(&proxy1_url).await?;
    let conn0 = server_handle.recv_connect().await?;
    client1.send("hello from proxy1").await?;
    assert_eq!(
        server_handle.recv_message().await?,
        (conn0, "hello from proxy1")
    );
    server_handle.send_to(conn0, "echo: hello from proxy1")?;
    assert_eq!(client1.recv().await?, "echo: hello from proxy1");
    client1.close().await?;
    server_handle.assert_disconnected(conn0).await?;

    // Connect to proxy 2 and send a message
    let proxy2_url = format!("ws://{}/", proxy2_addr);
    let mut client2 = WsTestClient::connect(&proxy2_url).await?;
    let conn1 = server_handle.recv_connect().await?;
    client2.send("hello from proxy2").await?;
    assert_eq!(
        server_handle.recv_message().await?,
        (conn1, "hello from proxy2")
    );
    server_handle.send_to(conn1, "echo: hello from proxy2")?;
    assert_eq!(client2.recv().await?, "echo: hello from proxy2");
    client2.close().await?;
    server_handle.assert_disconnected(conn1).await?;

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await?;

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file)?.as_str())?;
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.peer_id"# => "[peer_id]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.origin"# => "[origin]",
        r#".**.headers["sec-websocket-key"]"# => "[sec-websocket-key]",
        r#".**.headers["sec-websocket-version"]"# => "[sec-websocket-version]",
        r#".**.headers.connection"# => "[connection]",
        r#".**.headers.upgrade"# => "[upgrade]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from proxy1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from proxy1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": "Disconnect"
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from proxy2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from proxy2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": "Disconnect"
          }
        }
      ]
    }
    "#);
    Ok(())
}

#[tokio::test]
async fn test_recorder_ws_transform() -> Result<(), anyhow::Error> {
    let temp_file = tempfile::NamedTempFile::new()?;
    let test_file = temp_file.path().to_path_buf();
    let (server_addr, mut server_handle) = start_test_server().await?;
    let server_url = format!("ws://{}", server_addr);

    // Create recorder with transform/redact options
    let mut macaw = Macaw::<Recorder>::recorder();
    let options = WsProxyOptions {
        redact: Box::new(TestWsRedact),
        transform: Box::new(TestWsTransform),
        overrides: Default::default(),
    };
    let proxy_addr = macaw
        .add_ws_proxy("ws_proxy", "127.0.0.1:0", &server_url, options)
        .await?;

    let proxy_url = format!("ws://{}/", proxy_addr);
    let mut client = WsTestClient::connect(&proxy_url).await?;
    let conn_id = server_handle.recv_connect().await?;

    // Send message with transform encoding
    client.send(&encode_text("request1")).await?;
    let server_message_decoded = decode_text(server_handle.recv_message().await?.as_str()?);

    server_handle.send_to(
        conn_id,
        &encode_text(&format!("echo:{}", server_message_decoded)),
    )?;
    let response = client.recv().await?;

    assert_eq!(server_message_decoded, "request1");
    assert_eq!(response, encode_text("echo:request1"));

    // Send a message with secret text:
    client
        .send(&encode_text("This is a secret message"))
        .await?;
    let server_message_decoded = decode_text(server_handle.recv_message().await?.as_str()?);
    server_handle.send_to(
        conn_id,
        &encode_text(&format!("echo:{}", server_message_decoded)),
    )?;
    let response = client.recv().await?;

    assert_eq!(server_message_decoded, "This is a secret message");
    assert_eq!(response, encode_text("echo:This is a secret message"));

    client.close().await?;
    server_handle.assert_disconnected(conn_id).await?;

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await?;

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file)?.as_str())?;
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.peer_id"# => "[peer_id]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.origin"# => "[origin]",
        r#".**.headers["sec-websocket-key"]"# => "[sec-websocket-key]",
        r#".**.headers["sec-websocket-version"]"# => "[sec-websocket-version]",
        r#".**.headers.connection"# => "[connection]",
        r#".**.headers.upgrade"# => "[upgrade]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "request1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo:request1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "This is a REDACTED message"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo:This is a secret message"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": "Disconnect"
          }
        }
      ]
    }
    "#);
    Ok(())
}
