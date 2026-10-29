//go:build ignore

package main

import (
 "context"
 "encoding/json"
 "flag"
 "os"
 "time"
 "github.com/vphoenix/crypto-market-info/internal/replay"
 "github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func main() {
 atRaw:=flag.String("at","","RFC3339 UTC sample time")
 flag.Parse()
 ctx,cancel:=context.WithTimeout(context.Background(),5*time.Minute);defer cancel()
 c,err:=mysql.Open(ctx,mysql.Config{Socket:"/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock",Username:"root",PasswordFile:"/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password",Database:"crypto_market_info",ReadOnly:true,MaxConnections:1,SelectTimeout:12*time.Second});if err!=nil {panic(err)};defer c.Close()
 at:=time.Now().UTC().Add(-3*time.Minute).Truncate(time.Minute).Add(59*time.Second)
 if *atRaw!="" { at,err=time.Parse(time.RFC3339,*atRaw);if err!=nil{panic(err)} }
 snapshot,err:=c.LoadOptionsPlanSnapshot(ctx,at);if err!=nil {panic(err)}
 books:=[]map[string]any{};indexes:=map[string]any{};ids:=[]uint32{}
 for _,run:=range snapshot.Runs {
  for _,idx:=range run.Indexes {indexes[idx.IndexID]=idx.Samples[at.Second()]}
  for _,batch:=range run.Books {
   result,e:=replay.ReplayDerivative(batch,uint8(at.Second()))
   row:=map[string]any{"instrument_id":batch.InstrumentID,"run_id":run.RunID,"result":result}
   if e!=nil {row["error"]=e.Error()}
   books=append(books,row);ids=append(ids,batch.InstrumentID)
  }
 }
 _=ids
 instruments,e:=c.Instruments(ctx);if e!=nil {panic(e)}
 cex:=[]map[string]any{}
 for _,i:=range instruments {
  if i.ID!=1 && i.ID!=3 && i.ID!=5 && i.ID!=6 && i.ID!=7 {continue}
  book,valid,e:=c.ReplayBook(ctx,i.ID,at)
  row:=map[string]any{"instrument":i,"valid":valid,"book":book}
  if e!=nil {row["error"]=e.Error()};cex=append(cex,row)
 }
 out:=map[string]any{"observed_at_utc":time.Now().UTC(),"sample_at_utc":at,"plan_id":snapshot.Plan.ID,"expected_books":snapshot.ExpectedBooks,"valid_books":snapshot.ValidBooks,"missing_books":snapshot.MissingBooks,"missing_runs":snapshot.MissingRuns,"derivative_books":books,"indexes":indexes,"cex_books":cex}
 if e=json.NewEncoder(os.Stdout).Encode(out);e!=nil {panic(e)}
}
