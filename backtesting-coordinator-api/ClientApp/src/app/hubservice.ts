import { Injectable } from '@angular/core';
import * as signalR from "@microsoft/signalr";

@Injectable({
  providedIn: 'root'
})
export class HubService {
  log(name: any, message: any, queue: any, recsProcessed: any, completed: any): void {

    let proc = this.processors.filter(x => x.name == name)[0];
    proc.queueDepth = queue;
    proc.isCompleted = completed;

    if (name !== 'Coord')
      proc.recordsProcessed = recsProcessed;

    if (!this.logMessages[name])
      this.logMessages[name] = [];

    this.logMessages[name].push(message);

    this.processors[0].recordsProcessed = 0;
    this.processors.forEach(x => this.processors[0].recordsProcessed += x.recordsProcessed);
  }

  private hubConnection: signalR.HubConnection

  public startConnection = (callback) => {
    this.hubConnection = new signalR.HubConnectionBuilder()
      .withUrl('/coordination-hub')
      .build();
    this.hubConnection.serverTimeoutInMilliseconds = 100000; // 100 second

    this.hubConnection
      .start()
      .then((con) => {
        console.log('Connection started');
        this.ConnectUi();
        callback();
      })
      .catch(err => console.log('Error while starting connection: ' + err))

    this.hubConnection.on('UpdateProcessors', res => {
      this.processors.length = 1;
      this.processors.push(...res);
    });
    this.hubConnection.on('Log', (name, message, queue, recsProcessed, completed) => this.log(name, message, queue, recsProcessed, completed))
  }

  public ConnectUi = () => {
    return this.hubConnection.send('ConnectUi');
  }

  public RunModel = () => {
    return this.hubConnection.send('RunModel');
  }

  public processors: any[] = [{ name: 'Coord', isCoord: true, isCompleted: true }];
  public logMessages: any = {};
}
