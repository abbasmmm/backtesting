import { Component } from '@angular/core';
import { HubService } from '../hubservice';

@Component({
  selector: 'app-counter-component',
  templateUrl: './counter.component.html',
  styleUrls: ['./counter.component.css']
})
export class CounterComponent {
  public currentCount = 0;
  connected: boolean = false;
  constructor(public hub: HubService) {
    this.hub.startConnection(() => {
      this.connected = true;
      console.log('connected');
    });
  }

  get nodes() {
    return this.hub.processors;
  }

  logs(name) {
    console.log('getlog');
    return this.hub.logMessages[name];
  }

  get workerCount() {
    return this.hub.processors.length - 1;
  }

  public runModel() {
    this.hub.RunModel();
  }
}
