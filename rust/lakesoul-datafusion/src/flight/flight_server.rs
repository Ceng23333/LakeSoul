use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use std::sync::Arc;

// use arrow_flight::{PollInfo, SchemaAsIpc};
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use datafusion::prelude::*;

use arrow_flight::{
    flight_service_server::FlightService, 
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, SchemaAsIpc
};

use crate::Result;

#[derive(Clone)]
pub struct FlightServiceImpl {
    client: MetaDataClientRef
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        dbg!("get_schema");
        let request = request.into_inner();

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let table_path =
            ListingTableUrl::parse(&request.path[0]).map_err(to_tonic_err)?;

        let ctx = SessionContext::new();
        let schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .unwrap();

        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let schema_result = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        dbg!("do_get");
        let ticket = request.into_inner();
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                println!("do_get: {sql}");

                // create local execution context
                let ctx = SessionContext::new();

                let testdata = datafusion::test_util::parquet_test_data();

                // register parquet file with the execution context
                ctx.register_parquet(
                    "alltypes_plain",
                    &format!("{testdata}/alltypes_plain.parquet"),
                    ParquetReadOptions::default(),
                )
                .await
                .map_err(to_tonic_err)?;

                // create the DataFrame
                let df = ctx.sql(sql).await.map_err(to_tonic_err)?;

                // execute the query
                let schema = df.schema().clone().into();
                let results = df.collect().await.map_err(to_tonic_err)?;
                if results.is_empty() {
                    return Err(Status::internal("There were no results from ticket"));
                }

                // add an initial FlightData message that sends schema
                let options = arrow_ipc::writer::IpcWriteOptions::default();
                let schema_flight_data = SchemaAsIpc::new(&schema, &options);

                let mut flights = vec![FlightData::from(schema_flight_data)];

                let encoder = IpcDataGenerator::default();
                let mut tracker = DictionaryTracker::new(false);

                for batch in &results {
                    let (flight_dictionaries, flight_batch) = encoder
                        .encoded_batch(batch, &mut tracker, &options)
                        .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

                    flights.extend(flight_dictionaries.into_iter().map(Into::into));
                    flights.push(flight_batch.into());
                }

                let output = futures::stream::iter(flights.into_iter().map(Ok));
                Ok(Response::new(Box::pin(output) as Self::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        dbg!("handshake");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        dbg!("list_flights");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        dbg!("get_flight_info");

        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        dbg!("do_put");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        dbg!("do_action");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        dbg!("list_actions");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        dbg!("do_exchange");
        Err(Status::unimplemented("Not yet implemented"))
    }

    // async fn poll_flight_info(
    //     &self,
    //     _request: Request<FlightDescriptor>,
    // ) -> Result<Response<PollInfo>, Status> {
    //     Err(Status::unimplemented("Not yet implemented"))
    // }
}

impl FlightServiceImpl {
    pub async fn new() -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        Ok(FlightServiceImpl {
            client
        })
    }
}

fn to_tonic_err(e: datafusion::error::DataFusionError) -> Status {
    Status::internal(format!("{e:?}"))
}
