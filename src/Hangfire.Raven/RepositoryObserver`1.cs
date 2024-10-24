using System;

namespace Hangfire.Raven
{
  public class RepositoryObserver<T> : IObserver<T>
  {
    private Action<T> _action;

    public RepositoryObserver(Action<T> input) => this._action = input;

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(T value) => this._action(value);
  }
}
