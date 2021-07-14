import { Component, Inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { debug } from 'console';

@Component({
  selector: 'app-fetch-data',
  templateUrl: './fetch-data.component.html'
})
export class FetchDataComponent {
  public forecasts: WeatherForecast[];
  selected: any = "";
  models: any;

  constructor(private http: HttpClient, @Inject('BASE_URL') baseUrl: string) {
    this.http.get('api/get-models').subscribe(result => {
      this.models = result;
    }, error => console.error(error));
  }

  save() {
    if (this.selected.id) {
      this.http.put('api/update-model', this.selected).subscribe(x => alert('Saved successfully!!'), error => alert(error
      ))
    }
  }

  onChange(e) {
    if (e) {
      this.selected = this.models.filter(x => x.id == e)[0];
    }
    else
      this.selected = "";
    console.log(e);
  }
}

interface WeatherForecast {
  date: string;
  temperatureC: number;
  temperatureF: number;
  summary: string;
}
