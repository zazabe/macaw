use macaw::core::*;

#[derive(Debug)]
enum Command {
    Hello,
    GoodBye,
}

#[derive(Debug)]
enum Rec {
    HttpRequest(String),
    HttpResponse(Box<dyn RecordEvent>),
}

#[derive(Debug)]
struct State {
    name: String,
    count: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Foo(String);

#[typetag::serde]
impl RecordEvent for Foo {}

struct MyActor {
    state: State,
    messages: Vec<String>,
}

impl ActorHandler<Rec> for MyActor {
    type Reply = ();

    async fn handle(&mut self, message: Rec) {
        self.handle_record(&message).await;
        match message {
            Rec::HttpRequest(request) => {
                println!("HttpRequest: {}", request);
            }
            Rec::HttpResponse(response) => {
                println!("HttpResponse: {:?}", response);
            }
        }
    }
}

impl ActorHandler<Command> for MyActor {
    type Reply = String;

    async fn handle(&mut self, command: Command) -> String {
        self.handle_command(&command).await;
        match command {
            Command::Hello => {
                println!("Hello");
            }
            Command::GoodBye => {
                println!("GoodBye");
            }
        }
        "Response".to_string()
    }
}

impl Actor for MyActor {
    async fn on_start(&mut self, _context: &ActorContext) {
        println!("Actor started");
    }

    async fn on_stop(&mut self, _context: &ActorContext) {
        println!("Actor stopped");
    }
}

impl MyActor {
    fn new(name: String) -> Self {
        Self {
            state: State { name, count: 0 },
            messages: Vec::new(),
        }
    }

    async fn handle_command(&mut self, command: &Command) {
        match command {
            Command::Hello => {
                println!("Hello");
                self.state.count += 1;
            }
            Command::GoodBye => {
                println!("GoodBye");
            }
        }
    }

    async fn handle_record(&mut self, record: &Rec) {
        match record {
            Rec::HttpRequest(request) => {
                println!("HttpRequest: {}", request);
                self.messages.push(request.clone());
            }
            Rec::HttpResponse(response) => {
                println!("HttpResponse: {:?}", response);
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let local = tokio::task::LocalSet::new();
    local.block_on(&mut runtime, async {
        let context = ActorContext::new();

        let actor = MyActor::new("test".to_string());
        let handle = actor.run(&context);

        let v = handle.request(Command::Hello).await?;
        println!("Response: {}", v);
        handle.send(Rec::HttpRequest("Hi".to_string()))?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        handle.stop();
        let v = handle.request(Command::GoodBye).await?;
        println!("Response: {}", v);
        handle.send(Rec::HttpResponse(Box::new(Foo("Hello".to_string()))))?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        println!("Dropped tx");
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
