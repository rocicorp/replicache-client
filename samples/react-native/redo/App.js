import React, { Component } from 'react';
import {
  Button,
  Keyboard,
  Image,
  Text,
  TextInput,
  TouchableOpacity,
  View
} from "react-native";
import DraggableFlatList from 'react-native-draggable-flatlist'
import Replicant from 'replicant-react-native';
import { styles, viewPadding } from './styles.js'; 
 
export default class App extends Component {
 
  state = {
    root: '',
    text: "",
    todos: [],
  };

  async componentDidMount() {
    this._replicant = new Replicant('https://replicate.to/serve/react-native-susan');
    await this._initBundle();

    this._replicant.onChange = this._load;

    const root = await this._replicant.root();
    this.setState({
      root,
    });

    this._load();
  
    Keyboard.addListener(
      "keyboardWillShow",
      e => this.setState({ viewPadding: e.endCoordinates.height + viewPadding })
    );

    Keyboard.addListener(
      "keyboardWillHide",
      () => this.setState({ viewPadding: viewPadding })
    );
  }
 
  renderItem = ({ item, index, move, moveEnd, isActive }) => {
    return (
      <TouchableOpacity
        onLongPress={move}
        onPressOut={moveEnd}
      >
        <View key={item.id}>
          <View style={styles.listItemCont}>
            <Text style={[styles.listItem, { textDecorationLine: this._setTextDecorationLine(item.value.done)}]}
              onPress={() => this._handleDone(item.id, item.value.done)} >
              {item.value.title}
            </Text>
          <Button title="X" onPress={() => this._deleteTodo(item.id)} />
          </View>
          <View style={styles.hr} />
        </View>
      </TouchableOpacity>
    )
  }
 
  render() {
    return (
      <View style={[styles.container, { paddingBottom: this.state.viewPadding }]}>
        <TextInput
          ref="addTodoTextInput"
          style={styles.textInput}
          onChangeText={this._handleTextChange}
          onSubmitEditing={this._addTodo}
          value={this.state.text}
          placeholder="Add Tasks"
          returnKeyType="done"
          returnKeyLabel="done"
          autoFocus={true}
          autoCorrect={false}
        />
        <DraggableFlatList
          style={styles.list}
          data={this.state.todos}
          renderItem={this.renderItem}
          keyExtractor={item => item.id}
          scrollPercent={5}
          onMoveEnd={({ to, from }) => this._handleReorder(to, from)}
        />
      </View>
    )
  }

  _initBundle = async () => {
    const resource = require('./replicant.bundle');
    let resolved = Image.resolveAssetSource(resource).uri;

    // EEP. I'm not sure why resolveAssertSource insists on adding an '/assets' dir.
    // I think that it is stripped off internally when this is used with <Image>.
    resolved = resolved.replace('/assets', '');

    const resp = await (await fetch(resolved)).text();
    await this._replicant.putBundle(resp);
  }

  _load = async () => {
    let todos = await this._replicant.exec('getAllTodos');   
    
    // Sort todos by order.
    todos.sort(function(a, b){return a.value.order - b.value.order});
    
    this.setState({
      todos,
    });
  }

  _handleTextChange = text => {
    this.setState({ text: text });
  };

  _addTodo = async () => {
    let todos = this.state.todos;
    let text = this.state.text;
    let notEmpty = text.trim().length > 0;

    if (notEmpty) {
      const uid = await this._replicant.exec('uid');
      const index = todos.length == 0 ? 0 : todos.length;
      const order = this._getOrder(index);
      const done = false;
      await this._replicant.exec('addTodo', [uid, text, order, done]);
      this._load();
    }

    // Clear textinput field after todo has been added.
    this.setState({
      text: "",
    });
    
    // Set focus to textInput box after text has been submitted.
    this.refs.addTodoTextInput.focus();
  }

  // Calculates the order field by halving the distance between the left and right neighbor orders.
  // We do this so that order changes still make sense and behave sensibility when clients are making order changes offline.
  _getOrder = (index) => {
    const todos = this.state.todos;
    const minOrderValue = 0;
    const maxOrderValue = Number.MAX_VALUE;
    const leftNeighborOrder = index == 0 ? minOrderValue : todos[index-1].value.order;
    const rightNeighborOrder = index == todos.length ? maxOrderValue : todos[index].value.order;
    const order = leftNeighborOrder + ((rightNeighborOrder - leftNeighborOrder)/2);
    return order;
  }

  // Calculates the order field when items are reordered.
  _getReorder = (to, from) => {
    const isMoveup = from > to ? true : false;
    to = Math.min(to, this.state.todos.length - 1);
    const order = isMoveup ? this._getOrder(to) : this._getOrder(to+1);
    return order;
  }

  _setTextDecorationLine = (isDone) => {
    let textDecoration = 'none';
    if (isDone) textDecoration = 'line-through';

    return textDecoration;
  }

  _handleDone = async (key, prevDone) => {
    if (key != null) {
      let isDone = !prevDone;
      await this._replicant.exec('setDone', [key, isDone]);
    }
  };

  _deleteTodo = async (key) => {
    if (key != null) {
      await this._replicant.exec('deleteTodo', [key]);
    }
  };

  _handleReorder = async (to, from) => {
    const todos = this.state.todos;
    const id  = todos[from].id;
    const order = this._getReorder(to, from);
    await this._replicant.exec('setOrder', [id, order]);
  }

  _handleSync = async () => {
    const result = this._replicant.sync('https://replicate.to/serve/react-native-test');
  }
}
